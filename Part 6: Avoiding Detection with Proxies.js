const { chromium } = require('playwright');
const fs = require('fs');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

class Product {
  constructor(name, priceStr, url) {
    this.name = this.cleanName(name);
    this.priceGb = this.cleanPrice(priceStr);
    this.priceUsd = this.convertPriceToUsd(this.priceGb);
    this.url = this.createAbsoluteUrl(url);
  }

  cleanName(name) {
    if (name === " " || name === "" || name == null) {
      return "missing";
    }
    return name.trim();
  }

  cleanPrice(priceStr) {
    if (!priceStr) {
      return 0.0;
    }
    priceStr = priceStr.trim();
    priceStr = priceStr.replace("Sale price£", "");
    priceStr = priceStr.replace("Sale priceFrom £", "");
    if (priceStr === "") {
      return 0.0;
    }
    return parseFloat(priceStr);
  }

  convertPriceToUsd(priceGb) {
    return priceGb * 1.29;
  }

  createAbsoluteUrl(url) {
    if (url === "" || url == null) {
      return "missing";
    }
    return "https://www.chocolate.co.uk" + url;
  }
}

class ProductDataPipeline {
  constructor(jsonFileName = "", storageQueueLimit = 5) {
    this.seenProducts = new Set();
    this.storageQueue = [];
    this.jsonFileName = jsonFileName;
    this.jsonFileOpen = false;
    this.storageQueueLimit = storageQueueLimit;
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
    this.storageQueue = []; // Clear the queue after saving
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
      if (this.storageQueue.length >= this.storageQueueLimit) {
        this.saveToJson();
      }
    }
  }

  async close() {
    if (this.storageQueue.length > 0) {
      this.saveToJson();
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

async function makeScrapeOpsRequest(page, url) {
  const payload = {
    api_key: "<YOUR_SCRAPE_OPS_KEY>",
    url: encodeURIComponent(url),
  };

  const proxyUrl = `https://proxy.scrapeops.io/v1?${new URLSearchParams(
    payload
  ).toString()}`;

  return makeRequest(page, proxyUrl, 3, true);
}

async function scrape(url) {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  
  const response = await makeScrapeOpsRequest(page, url);
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