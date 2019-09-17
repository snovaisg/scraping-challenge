# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
import pandas as pd
import numpy as np

class ProductsSpider(scrapy.Spider):
    name = 'Products'
    allowed_domains = ['coolblue.be']
    start_urls = np.loadtxt("start_urls.txt",dtype='str').tolist()
    df = pd.DataFrame(columns = ['Name',"Url","nReviews","reviewsScore","CurrentPrice","OldPrice"])

    Rules = (Rule(LinkExtractor(allow=(), restrict_xpaths=('.//a[@rel="next"]',)), callback="parse", follow= True),\
    )

    def parse(self, response):
        print(f"Processing.. {response.url}")

        parser = scrapy.Selector(response)
        products = parser.xpath(".//div[@class='grid-unit-xs--col-8 grid-unit-l--col-12 js-product-details']")
        for idx,product in enumerate(products):
            XPATH_PRODUCT_TITLE = ".//div[@class='product__titles']//a[@class='product__title js-product-title']/@title"
            XPATH_PRODUCT_PARTIAL_LINK = ".//div[@class='product__titles']//a[@class='product__title js-product-title']/@href"
            XPATH_PRODUCT_REVIEWS = ".//div[@class='product__review-rating']//div[@class='review-rating']//span[@class='review-rating__reviews']//a[@class='review-rating__reviews-link']/text()"
            XPATH_PRODUCT_REVIEW_SCORE = ".//div[@class='product__review-rating']//div[@class='review-rating']//div[@class='review-rating__rating']//a[@class='review-rating__rating']//div[@class='review-rating__icons']//span[@class='review-rating__score']//meter[@class='review-rating__score-meter']/@value"
            XPATH_PRODUCT_CURRENT_PRICE = ".//div[@class='product__order-information']//div[@class='product__sales-information']//div[@class='product__sales-price-availability']//span[@class='sales-price sales-price--small sales-price--inline js-sales-price']//strong[@class='sales-price__current']/text()"
            XPATH_PRODUCT_FORMER_PRICE = ".//div[@class='product__order-information']//div[@class='product__sales-information']//div[@class='product__sales-price-availability']//span[@class='sales-price sales-price--small sales-price--inline js-sales-price']//span[@class='sales-price__former']/text()"

            raw_product_title = product.xpath(XPATH_PRODUCT_TITLE).extract()
            raw_product_url = product.xpath(XPATH_PRODUCT_PARTIAL_LINK).extract()
            raw_product_reviews = product.xpath(XPATH_PRODUCT_REVIEWS).extract()
            raw_product_reviews_score = product.xpath(XPATH_PRODUCT_REVIEW_SCORE).extract()
            raw_product_current_price = product.xpath(XPATH_PRODUCT_CURRENT_PRICE).extract()
            raw_product_former_price = product.xpath(XPATH_PRODUCT_FORMER_PRICE).extract()

            product_title = self.simpleClean(raw_product_title)
            product_url = f"{self.allowed_domains[0]}{''.join(raw_product_url).strip()}" if raw_product_url else None
            product_reviews = self.simpleClean(raw_product_reviews)
            product_reviews = [int(s) for s in product_reviews.split() if s.isdigit()][0] if product_reviews else None
            product_reviews_score = raw_product_reviews_score[0] if raw_product_reviews_score else None
            product_current_price = self.simpleClean(raw_product_current_price)
            product_former_price = self.simpleClean(raw_product_former_price)

            # Save to df
            self.df = self.df.append(pd.Series([product_title, product_url, product_reviews, product_reviews_score, product_current_price, product_former_price],index=self.df.columns), ignore_index=True)

        # follow next page links
        next_page = parser.xpath('.//a[@rel="next"]/@href').extract()
        if next_page:
            next_href = next_page[0]
            # find out the what is the current main url
            current_page_url = [url for url in self.start_urls if url in response.url][0]
            next_page_url = current_page_url + next_href
            request = scrapy.Request(url=next_page_url)
            yield request
        else:
            self.df.to_csv("Products.csv")

    def simpleClean(self,raw_text):
        return ''.join(raw_text).strip() if raw_text else None
