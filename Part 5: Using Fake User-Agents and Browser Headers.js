const { chromium } = require('playwright');
const fs = require('fs');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const axios = require('axios');


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
const scrapeOpsKey = "<YOUR_SCRAPE_OPS_KEY>";

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

async function getHeaders(numHeaders) {
  const fallbackHeaders = [
     {
      "upgrade-insecure-requests": "1",
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Windows; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
      accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
      "sec-ch-ua":
        '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"Windows"',
      "sec-fetch-site": "none",
      "sec-fetch-mod": "",
      "sec-fetch-user": "?1",
      "accept-encoding": "gzip, deflate, br",
      "accept-language": "bg-BG,bg;q=0.9,en-US;q=0.8,en;q=0.7",
    },
    {
      "upgrade-insecure-requests": "1",
      "user-agent":
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36",
      accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
      "sec-ch-ua":
        '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"Linux"',
      "sec-fetch-site": "none",
      "sec-fetch-mod": "",
      "sec-fetch-user": "?1",
      "accept-encoding": "gzip, deflate, br",
      "accept-language": "fr-CH,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    },
  ];


  try {
    const response = await axios.get(
      `http://headers.scrapeops.io/v1/browser-headers?api_key=${scrapeOpsKey}&num_results=${numHeaders}`
    );

    if (response.data.result.length > 0) {
      return response.data.result;
    } else {
      console.error("No headers from ScrapeOps, using fallback headers");
      return fallbackHeaders;
    }
  } catch (error) {
    console.error(
      "Failed to fetch headers from ScrapeOps, using fallback headers"
    );
    return fallbackHeaders;
  }
}

async function scrape(url, headers) {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    extraHTTPHeaders: headers
  });
  
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
  const pipeline = new ProductDataPipeline("chocolate.csv", 5);
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
  let headers = [];

  const handleWork = async (workUrl) => {
    if (headers.length == 0) {
      headers = await getHeaders(2);
    }
    const { nextUrl, products } = await scrape(
        workUrl, 
        headers[Math.floor(Math.random() * headers.length)]
    );
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