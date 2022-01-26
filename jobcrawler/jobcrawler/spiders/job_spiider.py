# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.

import json
from tkinter import EXCEPTION
import scrapy
import re
import logging
from scrapy.crawler import CrawlerProcess


class Jobcrawler(scrapy.Spider):
    # JobApi URL
    job_api = 'https://www.jobs.ch/api/v1/public/search/job/'
    company_api = 'https://www.jobs.ch/api/v1/public/company/'

    # CSS SELECTORS
    regions = '#skip-link-target > div > main > div.sc-dkPtRN.cbLTeC > div > div.sc-dkPtRN.bFtyxf > div > ul > li > a::attr(href)'
    pagination_option_1 = '#skip-link-target > div.sc-dkPtRN.cfmXsY > div > main > div.sc-dkPtRN.Flex-mjmi48-0.VacancyGrid__StyledLeft-kpoi2l-1.jZsXcm.kIybRh > div:nth-child(2) > div.sc-dkPtRN.etoyEp > div > div > a:nth-child(4)::attr(href)'
    pagination_option_2 = '#skip-link-target > div.sc-dkPtRN.cfmXsY > div > main > div.sc-dkPtRN.Flex-mjmi48-0.VacancyGrid__StyledLeft-kpoi2l-1.jZsXcm.kIybRh > div:nth-child(2) > div.sc-dkPtRN.etoyEp > div > div > a ::attr(href)'
    job_item = '.sc-dkPtRN.VacancySerpItem__ShadowBox-ppntto-0.dyhCGY a::attr(href)'

    name = "jobs"  # Name of the spider

    # The very begining
    start_urls = ["https://www.jobs.ch/de/stellenangebote/"]

    logger = logging.getLogger()

    def parse(self, response):  # TASK 1 - Fetching all "Regionen" an dLoop trow it
        for region in response.css(self.regions):
            url_nextPage = response.urljoin(region.extract())
            yield scrapy.Request(url_nextPage, callback=self.task_1)

    def task_1(self, response):  # TASk 2 - GET all Job Ids

        if 'page=' in response.request.url and not 'page=100' in response.request.url:
            url_nextPage = response.css(self.pagination_option_1).get()
        else:
            url_nextPage = response.css(self.pagination_option_2).get()

        for item in response.css(self.job_item):
            job_id = re.findall(r'\d+', item.get())[0]
            api_url = response.urljoin(self.job_api + job_id)

            yield scrapy.Request(api_url, callback=self.task_2, errback=self.errback_httpbin, dont_filter=True)

        url = response.urljoin(url_nextPage)
        # if has more pages go to otther page and loop
        yield scrapy.Request(url, callback=self.task_1, errback=self.errback_httpbin, dont_filter=True)

    def task_2(self, response):  # get the final data (some of them are none)
        jsonresponse = json.loads(response.text)

        item = {}
        errorMessage = " => !!!!!!!! Wert wurde nicht gefunden !!!!!!!!"

        check_mail = "application_email" in jsonresponse

        if check_mail:
            try:
                item['title'] = jsonresponse["title"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
                self.logger.warning(e)

            try:
                item["company_visible"] = jsonresponse["company_visible"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)

            try:
                item["slug"] = jsonresponse["slug"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["application_method"] = jsonresponse["application_method"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["application_email"] = jsonresponse["application_email"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["job_source_type"] = jsonresponse["job_source_type"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["last_online_date"] = jsonresponse["last_online_date"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["job_id"] = jsonresponse["job_id"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["datapool_id"] = jsonresponse["datapool_id"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["company_name"] = jsonresponse["company_name"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["company_id"] = jsonresponse["company_id"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["industry_id"] = jsonresponse["industry_id"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["publication_date"] = jsonresponse["publication_date"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["initial_publication_date"] = jsonresponse["initial_publication_date"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["place"] = jsonresponse["place"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["street"] = jsonresponse["street"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["external_url"] = jsonresponse["external_url"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["zipcode"] = jsonresponse["zipcode"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["synonym"] = jsonresponse["synonym"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["template_profession"] = jsonresponse["template_profession"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["template_text"] = jsonresponse["template_text"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["template_strip"] = jsonresponse["template_text"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["template_contact_address"] = jsonresponse["template_contact_address"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["offer_id"] = jsonresponse["offer_id"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["is_active"] = jsonresponse["is_active"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["is_responsive"] = jsonresponse["is_responsive"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["is_paid"] = jsonresponse["is_paid"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["headhunter_application_allowed"] = jsonresponse["headhunter_application_allowed"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["contact_person"] = jsonresponse["contact_person"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["categorie_1"] = jsonresponse["categories"][0]["lvl_0"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["categorie_2"] = jsonresponse["categories"][0]["lvl_1"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["region_1"] = jsonresponse["regions"][0]["lvl_0"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["region_2"] = jsonresponse["regions"][0]["lvl_1"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["region_3"] = jsonresponse["regions"][0]["lvl_2"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)

            try:
                item["source_hostname"] = jsonresponse["source_hostname"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["template_lead_text"] = jsonresponse["template_lead_text"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["company_slug"] = jsonresponse["company_slug"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["company_name"] = jsonresponse["company_name"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                jsonresponse["images"]
                if jsonresponse["images"]:
                    item["image"] = jsonresponse["images"][0]["url"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["lat"] = jsonresponse["coordinates"]["lat"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)
            try:
                item["lon"] = jsonresponse["coordinates"]["lon"]
            except Exception as e:
                self.logger.debug(
                    str(e) + errorMessage)

            # get company Fields of the job obove

            try:
                company_id = item["company_id"]
                api_url = response.urljoin(self.company_api + company_id)
                yield scrapy.Request(api_url, callback=self.task_3, errback=self.errback_httpbin, dont_filter=True, meta={'item': item})
            except Exception as e:
                self.logger.debug(
                    "!!!!!!!! keine Company ID  !!!!!!!!")

    def task_3(self, response):  # get the final data (some of them are none)
        jsonresponse = json.loads(response.text)
        item = response.meta['item']

        try:
            item['company_api_id'] = jsonresponse["id"]
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item['company_api_name'] = jsonresponse["name"]
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item['company_api_portrait_descriptions_search_de'] = jsonresponse["portrait_descriptions_search"]["de"]
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item['company_api_portrait_descriptions_search_en'] = jsonresponse["portrait_descriptions_search"]["en"]
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item['company_api_portrait_descriptions_search_fr'] = jsonresponse["portrait_descriptions_search"]["fr"]
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item['company_api_slug'] = jsonresponse["slug"]
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_is_visible"] = jsonresponse["is_visible"]
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_datapool_id"] = jsonresponse["datapool_id"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_segmentation"] = jsonresponse["segmentation"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_industry"] = jsonresponse["industry"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_founding_year"] = jsonresponse["founding_year"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_hiring_teaser"] = jsonresponse["hiring_teaser"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_url"] = jsonresponse["url"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_addresses_street1"] = jsonresponse["addresses"][0]["street1"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_addresses_street2"] = jsonresponse["addresses"][0]["street2"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_addresses_city"] = jsonresponse["addresses"][0]["city"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_addresses_zip_code"] = jsonresponse["addresses"][0]["zip_code"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_addresses_country_code"] = jsonresponse["addresses"][0]["country_code"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_addresses_telephone1"] = jsonresponse["addresses"][0]["telephone1"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_addresses_telephone2"] = jsonresponse["addresses"][0]["telephone2"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_addresses_email"] = jsonresponse["addresses"][0]["email"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)

        try:
            item["company_api_addresses_coordinates_lat"] = jsonresponse["addresses"][0]["coordinates"]["lat"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)
        try:
            item["company_api_addresses_coordinates_lon"] = jsonresponse["addresses"][0]["coordinates"]["lon"],
        except Exception as e:
            self.logger.warning(
                str(e) + self.errorMessage)

        return item

    def errback_httpbin(self, failure):
        # log all failures
        self.logger.debug("!!!!!!!! Request Fehler !!!!!!!!")


process = CrawlerProcess(

    settings={
        'FEED_URI': 'autocrawl.json',
        'FEED_FORMAT': 'json'
    }
)

process.crawl(Jobcrawler)
process.start()
