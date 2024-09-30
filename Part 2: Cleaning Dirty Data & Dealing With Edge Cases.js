const { chromium } = require('playwright');
const fs = require('fs');

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
  constructor(csvFilename = "", storageQueueLimit = 5) {
    this.seenProducts = new Set();
    this.storageQueue = [];
    this.csvFilename = csvFilename;
    this.csvFileOpen = false;
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
    this.storageQueue = [];
    this.csvFileOpen = false;
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

  addProduct(rawProduct) {
    const product = this.cleanRawProduct(rawProduct);
    if (!this.isDuplicateProduct(product)) {
      this.storageQueue.push(product);
      if (
        this.storageQueue.length >= this.storageQueueLimit &&
        !this.csvFileOpen
      ) {
        this.saveToCsv();
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
  const pipeline = new ProductDataPipeline("chocolate.csv", 5);
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
        pipeline.addProduct({
          name: rawProduct.title,
          price: rawProduct.price,
          url: rawProduct.url
        });
      }
    }

    await nextPage(page);
  }

  await pipeline.close();
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

// Scraping: https://www.chocolate.co.uk/collections/all
// Scraping: https://www.chocolate.co.uk/collections/all?page=2
// Scraping: https://www.chocolate.co.uk/collections/all?page=3
// Last Page Reached