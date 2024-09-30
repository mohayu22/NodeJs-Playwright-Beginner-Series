const { chromium } = require("playwright");
const fs = require("fs");

const listOfUrls = ["https://www.chocolate.co.uk/collections/all"];
const scrapedData = [];

async function scrape() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  for (let url of listOfUrls) {
    console.log(`Scraping: ${url}`);
    await page.goto(url);

    const productItems = await page.$$eval("product-item", items =>
      items.map(item => ({
        title: item.querySelector(".product-item-meta__title")?.textContent.trim() || null,
        price: item.querySelector(".price")?.textContent.replace("Sale price£", "").trim() || null,
        url: item.querySelector(".product-item-meta__title")?.getAttribute("href") || null
      }))
    );

    scrapedData.push(...productItems);
    await nextPage(page);
  }

  await browser.close();
  saveAsCSV(scrapedData, 'scraped_data.csv');
}

function saveAsCSV(data, filename) {
  if (data.length === 0) {
    console.log("No data to save.");
    return;
  }

  const header = Object.keys(data[0]).join(",");
  const csv = [header, ...data.map((obj) => Object.values(obj).join(","))].join("\n");
  fs.writeFileSync(filename, csv);
  console.log(`Data saved to ${filename}`);
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
// Data saved to scraped_data.csv