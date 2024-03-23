from twisted.internet import asyncioreactor
asyncioreactor.install()

import scrapy
from scrapy.crawler import CrawlerProcess
import csv
import re

class YearFinderSpider(scrapy.Spider):
    name = 'yearsfinal'
    allowed_domains = ['snacknation.com']  # Ensure this matches the domain you're scraping
    custom_settings = {
        'DOWNLOAD_DELAY': 1,  # Throttle requests to avoid being banned
        'LOG_LEVEL': 'ERROR',  # Only log errors to keep output clean
        'FEED_FORMAT': 'csv',  # Output format
        'FEED_URI': 'incompleteyears.csv',  # Output file
    }

    years = ['2019', '2020', '2021', '2022', '2023']
    year_regex = re.compile('|'.join(years))

    def start_requests(self):
        """Read URLs from a CSV file and initiate requests."""
        with open('urls.csv', 'r') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                url = row[0].strip()
                yield scrapy.Request(url, self.parse, errback=self.error_handler)

    def parse(self, response):
        """Parse the content of the response."""
        if 'text/html' not in response.headers.get('Content-Type', b'').decode():
            self.logger.info(f"Skipped non-text response from {response.url}")
            return

        content_sources = [
            ('title', response.xpath('//title/text()').get()),
            ('meta_description', response.xpath('//meta[@name="description"]/@content').get()),
            ('og_title', response.xpath('//meta[@property="og:title"]/@content').get()),
            ('og_description', response.xpath('//meta[@property="og:description"]/@content').get()),
            ('og_url', response.xpath('//meta[@property="og:url"]/@content').get()),
            ('og_image', response.xpath('//meta[@property="og:image"]/@content').get()),
            ('og_type', response.xpath('//meta[@property="og:type"]/@content').get()),
            ('twitter_card', response.xpath('//meta[@name="twitter:card"]/@content').get()),
            ('twitter_title', response.xpath('//meta[@name="twitter:title"]/@content').get()),
            ('twitter_description', response.xpath('//meta[@name="twitter:description"]/@content').get()),
            ('twitter_image', response.xpath('//meta[@name="twitter:image"]/@content').get()),
            ('image_src', response.xpath('//img/@src').get()),
            ('image_alt', response.xpath('//img/@alt').get()),
            ('main_text', ' '.join(response.xpath('//article//p/text()').getall()))
        ]

        for tag, content in content_sources:
            if content:
                found_years = self.year_regex.findall(content)
                for year in found_years:
                    if tag == 'main_text':
                        start_indices = [m.start() for m in re.finditer(year, content)]
                        for start in start_indices:
                            start, end = max(start - 50, 0), start + 50 + len(year)
                            surrounding_text = content[start:end]
                            yield {
                                'year': year,
                                'url': response.url,
                                'text': f"{tag}: {surrounding_text}"
                            }
                    else:
                        yield {
                            'year': year,
                            'url': response.url,
                            'text': f"{tag}: {content[:100]}"  # Show first 100 chars for non-main_text tags
                        }

    def error_handler(self, failure):
        """Log errors encountered during request processing."""
        self.logger.error(f"Request failed for {failure.request.url}: {failure}")

# Initialize and start the crawler process
process = CrawlerProcess()
process.crawl(YearFinderSpider)
process.start()
