const { chromium } = require('playwright');
const fs = require('fs');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

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

async function makeRequest(page, url, retries = 3, antiBotCheck = false) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await page.goto(url);
      const status = response.status();
      if ([200, 404].includes(status)) {
        if (antiBotCheck && status == 200) {
          const content = await page.content();
          if (content.includes("<title>Robot or human?</title>")) {
            return null;
          }
        }
        return response;
      }
    } catch (e) {
      console.log(`Failed to fetch ${url}, retrying...`);
    }
  }
  return null;
}

async function scrape(url) {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  
  const response = await makeRequest(page, url);
  if (!response) {
    await browser.close();
    return { nextUrl: null, products: [] };
  }

  const productItems = await page.$$eval("product-item", items =>
    items.map(item => {
      const titleElement = item.querySelector(".product-item-meta__title");
      const priceElement = item.querySelector(".price");
      return {
        name: titleElement ? titleElement.textContent.trim() : null,
        price: priceElement ? priceElement.textContent.trim() : null,
        url: titleElement ? titleElement.getAttribute("href") : null
      };
    })
  );

  const nextUrl = await nextPage(page);
  await browser.close();

  return {
    nextUrl: nextUrl,
    products: productItems.filter(item => item.name && item.price && item.url)
  };
}

async function nextPage(page) {
  let nextUrl = null;
  try {
    nextUrl = await page.$eval("a.pagination__nav-item:nth-child(4)", item => item.href);
  } catch (error) {
    console.log('Last Page Reached');
  }
  return nextUrl;
}

if (isMainThread) {
  const pipeline = new ProductDataPipeline("chocolate.json", 5);
  const workers = [];

  for (const url of listOfUrls) {
    workers.push(
      new Promise((resolve, reject) => {
        const worker = new Worker(__filename, {
          workerData: { startUrl: url }
        });
        console.log("Worker created", worker.threadId, url);

        worker.on("message", (product) => {
          pipeline.addProduct(product);
        });

        worker.on("error", reject);
        worker.on("exit", (code) => {
          if (code !== 0) {
            reject(new Error(`Worker stopped with exit code ${code}`));
          } else {
            console.log("Worker exited");
            resolve();
          }
        });
      })
    );
  }

  Promise.all(workers)
    .then(() => pipeline.close())
    .then(() => console.log("Pipeline closed"));
} else {
  const { startUrl } = workerData;
  const handleWork = async (workUrl) => {
    const { nextUrl, products } = await scrape(workUrl);
    for (const product of products) {
      parentPort.postMessage(product);
    }

    if (nextUrl) {
      console.log("Worker working on", nextUrl);
      await handleWork(nextUrl);
    }
  };

  handleWork(startUrl).then(() => console.log("Worker finished"));
}

// Worker created 1 https://www.chocolate.co.uk/collections/all
// Worker working on https://www.chocolate.co.uk/collections/all?page=2
// Worker working on https://www.chocolate.co.uk/collections/all?page=3
// Last Page Reached
// Worker finished
// Worker exited
// Pipeline closed