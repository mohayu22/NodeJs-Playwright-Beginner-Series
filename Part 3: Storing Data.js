const { chromium } = require('playwright');
const fs = require('fs');
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const mysql = require("mysql");
const { Client } = require("pg");


class Product {
  constructor(name, priceStr, url, conversionRate = 1.32) {
    this.name = this.cleanName(name);
    this.priceGb = this.cleanPrice(priceStr);
    this.priceUsd = this.convertPriceToUsd(this.priceGb, conversionRate);
    this.url = this.createAbsoluteUrl(url);
  }

  cleanName(name) {
    return name?.trim() || "missing";
  }

  cleanPrice(priceStr) {
    if (!priceStr?.trim()) {
      return 0.0;
    }

    const cleanedPrice = priceStr
      .replace(/Sale priceFrom £|Sale price£/g, "")
      .trim();

    return cleanedPrice ? parseFloat(cleanedPrice) : 0.0;
  }

  convertPriceToUsd(priceGb, conversionRate) {
    return priceGb * conversionRate;
  }

  createAbsoluteUrl(url) {
    return (url?.trim()) ? `https://www.chocolate.co.uk${url.trim()}` : "missing";
  }
}

class ProductDataPipeline {
  constructor(
    csvFilename = "",
    jsonFileName = "",
    s3Bucket = "",
    mysqlDbName = "",
    pgDbName = "",
    storageQueueLimit = 5
  ) {
    this.seenProducts = new Set();
    this.storageQueue = [];
    this.csvFilename = csvFilename;
    this.csvFileOpen = false;
    this.jsonFileName = jsonFileName;
    this.s3Bucket = s3Bucket;
    this.mysqlDbName = mysqlDbName;
    this.pgDbName = pgDbName;
    this.storageQueueLimit = storageQueueLimit;
  }

  saveToCsv() {
    this.csvFileOpen = true;
    const fileExists = fs.existsSync(this.csvFilename);
    const file = fs.createWriteStream(this.csvFilename, { flags: "a" });
    if (!fileExists) {
      file.write("name,priceGb,priceUsd,url\n");
    }
    for (const product of this.storageQueue) {
      file.write(
        `${product.name},${product.priceGb},${product.priceUsd},${product.url}\n`
      );
    }
    file.end();
    this.csvFileOpen = false;
  }

  saveToJson() {
    if (this.storageQueue.length <= 0) {
      return;
    }

    const fileExists = fs.existsSync(this.jsonFileName);
    let existingData = [];
    if (fileExists) {
      const fileContent = fs.readFileSync(this.jsonFileName, "utf8");
      existingData = JSON.parse(fileContent);
    }

    const mergedData = [...existingData, ...this.storageQueue];
    fs.writeFileSync(this.jsonFileName, JSON.stringify(mergedData, null, 2));
  }

  async saveToS3Bucket() {
    const client = new S3Client({
      region: "us-east-1",
      credentials: {
        accessKeyId: "YOUR_ACCESS_KEY_ID",
        secretAccessKey: "YOUR_SECRET_ACCESS_KEY",
      },
    });

    let retry = 3;
    while (retry > 0) {
      if (!fs.existsSync(this.csvFilename)) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
      retry -= 1;
    }

    const params = {
      Bucket: this.s3Bucket,
      Key: this.csvFilename,
      Body: fs.createReadStream(this.csvFilename),
    };

    await client.send(new PutObjectCommand(params));
  }

  saveToMysql() {
    if (this.storageQueue.length <= 0) {
      return;
    }

    return new Promise((resolve, reject) => {
      const connection = mysql.createConnection({
        host: "localhost",
        user: "root",
        password: "password",
        database: this.mysqlDbName,
      });

      connection.connect((err) => {
        if (err) {
          console.error("Error connecting to database: ", err);
          reject(err);
          return;
        }

        const query =
          "INSERT INTO chocolate_products (name, price_gb, price_usd, url) VALUES ?";
        const values = this.storageQueue.map((product) => [
          product.name,
          product.priceGb,
          product.priceUsd,
          product.url,
        ]);
        connection.query(query, [values], (err, results) => {
          if (err) {
            console.error("Error inserting data into database: ", err);
            connection.end();
            reject(err);
          } else {
            connection.end();
            resolve(results);
          }
        });
      });
    });
  }

  async saveToPostgres() {
    if (this.storageQueue.length <= 0) {
      return;
    }

    const client = new Client({
      user: "postgres",
      host: "localhost",
      database: this.pgDbName,
      password: "mysecretpassword",
      port: 5432,
    });

    try {
      await client.connect();
      const query =
        "INSERT INTO chocolate_products (name, price_gb, price_usd, url) VALUES ($1, $2, $3, $4)";
      for (const product of this.storageQueue) {
        await client.query(query, [
          product.name,
          product.priceGb,
          product.priceUsd,
          product.url,
        ]);
      }
    } catch (error) {
    } finally {
      await client.end();
    }
  }


  cleanRawProduct(rawProduct) {
    return new Product(rawProduct.name, rawProduct.price, rawProduct.url);
  }

  isDuplicateProduct(product) {
    if (!this.seenProducts.has(product.url)) {
      this.seenProducts.add(product.url);
      return false;
    }
    return true;
  }

  async addProduct(rawProduct) {
    const product = this.cleanRawProduct(rawProduct);
    if (!this.isDuplicateProduct(product)) {
      this.storageQueue.push(product);
      if (
        this.storageQueue.length >= this.storageQueueLimit &&
        !this.csvFileOpen
      ) {
        this.saveToCsv();
        this.saveToJson();
        this.saveToMysql();
        await this.saveToPostgres();
        this.storageQueue = [];
      }
    }
  }

  async close() {
    while (this.csvFileOpen) {
      // Wait for the file to be written
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    if (this.storageQueue.length > 0) {
      this.saveToCsv();
    }
  }
}

const listOfUrls = ["https://www.chocolate.co.uk/collections/all"];

async function scrape() {
  const pipeline = new ProductDataPipeline(
    "chocolate.csv",
    "chocolate.json",
    "chocolate-bucket",
    "chocolate_db",
    "chocolate_db",
    5
  );
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  for (let url of listOfUrls) {
    console.log(`Scraping: ${url}`);
    await page.goto(url);

    const productItems = await page.$$eval("product-item", items =>
      items.map(item => {
        const titleElement = item.querySelector(".product-item-meta__title");
        const priceElement = item.querySelector(".price");
        return {
          title: titleElement ? titleElement.textContent.trim() : null,
          price: priceElement ? priceElement.textContent.trim() : null,
          url: titleElement ? titleElement.getAttribute("href") : null
        };
      })
    );

    for (const rawProduct of productItems) {
      if (rawProduct.title && rawProduct.price && rawProduct.url) {
        await pipeline.addProduct({
          name: rawProduct.title,
          price: rawProduct.price,
          url: rawProduct.url
        });
      }
    }

    await nextPage(page);
  }

  await pipeline.close();
  await pipeline.saveToS3Bucket();
  await browser.close();
}

async function nextPage(page) {
  let nextUrl;
  try {
    nextUrl = await page.$eval("a.pagination__nav-item:nth-child(4)", item => item.href);
  } catch (error) {
    console.log('Last Page Reached');
    return;
  }
  listOfUrls.push(nextUrl);
}

(async () => {
  await scrape();
})();