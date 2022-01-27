# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface


# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a item interface
from asyncio.log import logger
from operator import getitem
from sqlite3 import connect
from itemadapter import ItemAdapter
import mysql.connector
import mysql
import os
import w3lib.html
import logging


# class JobsChPipeline:
#   def process_item(self, item, spider):
#      return item


class JobcrawlerPipeline (object):

    logger = logging.getLogger()

    # DB Vars!
    db_host = os.environ.get('HOST')
    db_port = os.environ.get('PORT')
    db_user = os.environ.get('USER')
    db_passwd = os.environ.get('PWD')
    db_database = os.environ.get('DB')
    db_table_jobs = os.environ.get('TABLE_JOBS')
    db_table_companys = os.environ.get('TABLE_COMPANYS')

    def __init__(self):
        self.create_connection()
        self.create_table_jobs()
        self.create_table_companys()

    def create_connection(self):
        self.logger.info(" --------- CREATE CONNECTION --------- ")
        self.conn = mysql.connector.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            passwd=self.db_passwd,
            database=self.db_database
        )
        self.curr = self.conn.cursor(buffered=True)

    def create_table_jobs(self):
        self.logger.info(" --------- CREATE TABLE JOBS--------- ")
        self.curr.execute(""" CREATE TABLE IF NOT EXISTS """ + self.db_table_jobs + """ (
            id int(11) NOT NULL AUTO_INCREMENT,
            job_id varchar(255),
            title text,
            company_visible boolean,
            slug text,
            company_slug text,
            source_hostname text,
            application_method text,
            application_email varchar(255),
            job_source_type text,
            last_online_date text,
            datapool_id varchar(255),
            company_name text,
            company_id varchar(255),
            industry_id varchar(255),
            publication_date text,
            initial_publication_date text,
            place text,
            street text,
            external_url text,
            zipcode varchar(255),
            synonym text,
            template_profession text,
            template_text text,
            template_strip text,
            template_lead_text text,
            template_contact_address text,
            offer_id varchar(255),
            is_active boolean,
            is_responsive boolean,
            is_paid boolean,
            image varchar(255),
            cat_1 varchar(255),
            cat_2 varchar(255),
            reg_1 varchar(255),
            reg_2 varchar(255),
            reg_3 varchar(255),
            lat varchar(255),
            lon varchar(255),


            PRIMARY KEY (id)

        ) """)

    def create_table_companys(self):
        self.logger.info(" --------- CREATE TABLE COMPANY --------- ")
        self.curr.execute(""" CREATE TABLE IF NOT EXISTS """ + self.db_table_companys + """ (
            id int(11) NOT NULL AUTO_INCREMENT,
            company_id varchar(255),
            title text,
            descriptions_search_de text,
            descriptions_search_en text,
            descriptions_search_fr text,
            slug text,
            is_visible boolean,
            datapool_id varchar(255),
            segmentation text,
            industry int,
            founding_year text,
            hiring_teaser text,
            url text,
            street1 text,
            street2 text,
            city text,
            zip_code text,
            country_code text,
            telephone1 varchar(255),
            telephone2 varchar(255),
            email varchar(255),
            lat varchar(255),
            lon varchar(255),
            PRIMARY KEY (id)

        ) """)

    def process_item(self, item, spider):
        self.logger.debug(" --------- PROCESS DATA --------- ")
        self.store_db(item)
        return item

    def store_db(self, item):

        try:
            src_hostname = item["source_hostname"]
        except Exception:
            src_hostname = None
        try:
            image = item["image"],
        except Exception:
            image = None

        try:
            lat = item["lat"],
        except Exception:
            lat = None

        try:
            long = item["lon"],
        except Exception:
            long = None

        try:
            temp_lead_text = item["template_lead_text"],
        except Exception:
            temp_lead_text = None

        try:
            temp_lead_text = item["template_lead_text"],
        except Exception:
            temp_lead_text = None

        add_data_job = ("INSERT INTO " + self.db_table_jobs + ""
                        "(id, job_id, title, company_visible, slug, company_slug, source_hostname, application_method, application_email, job_source_type, last_online_date, datapool_id, company_name, company_id, industry_id,  publication_date, initial_publication_date, place,street,external_url, zipcode,synonym,template_profession, template_text, template_strip, template_contact_address, offer_id, is_active, is_responsive, is_paid, cat_1, cat_2, reg_1, reg_2, reg_3, image, template_lead_text, lat, lon )"
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s)")

        values_job = (0, item["job_id"], item["title"], item["company_visible"], item["slug"], item["company_slug"], src_hostname, item["application_method"], item["application_email"], item["job_source_type"],
                      item["last_online_date"], item["datapool_id"], item["company_name"], item["company_id"], item[
                      'industry_id'],  item["publication_date"], item["initial_publication_date"], item["place"], item["street"], item["external_url"], item["zipcode"], item["synonym"], item["template_profession"], item["template_text"], w3lib.html.remove_tags(item["template_strip"]), item["template_contact_address"], item["offer_id"], item["is_active"], item["is_responsive"], item["is_paid"], item["categorie_1"],
                      item["categorie_2"],
                      item["region_1"],
                      item["region_2"],
                      item["region_3"],
                      str(image),

                      str(temp_lead_text),
                      str(lat),
                      str(long),
                      )

        add_data_company = ("INSERT INTO " + self.db_table_companys + ""
                            "(id, company_id, title, descriptions_search_de, descriptions_search_en, descriptions_search_fr, slug, is_visible, datapool_id, segmentation, industry, founding_year, hiring_teaser, url, street1, street2, city, zip_code, country_code, telephone1, telephone2, email, lat, lon)"
                            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ")

        values_company = (0, item["company_api_id"], item["company_api_name"], item['company_api_portrait_descriptions_search_de'], item['company_api_portrait_descriptions_search_en'], item['company_api_portrait_descriptions_search_fr'], item['company_api_slug'], item["company_api_is_visible"], item["company_api_datapool_id"][0], item["company_api_segmentation"][0], item["company_api_industry"][0], item["company_api_founding_year"][0], item["company_api_hiring_teaser"][0],
                          item["company_api_url"][0], item["company_api_addresses_street1"][0], item["company_api_addresses_street2"][0], item["company_api_addresses_city"][0], item["company_api_addresses_zip_code"][0], item["company_api_addresses_country_code"][0], item["company_api_addresses_telephone1"][0], item["company_api_addresses_telephone2"][0], item["company_api_addresses_email"][0], item["company_api_addresses_coordinates_lat"][0], item["company_api_addresses_coordinates_lon"][0])

        # my_connect is the connection
        row_count_cursor = self.conn.cursor(buffered=True)
        row_count_cursor.execute(
            "SELECT * FROM  jobs_ch WHERE job_id  = " + item["job_id"])

        if row_count_cursor.rowcount > 0:
            self.logger.warning(
                "  --------- JOB (" + item["job_id"] + ") IS DUPLIICATE IN " + self.db_table_jobs + " --------- ")

        else:
            try:
                self.curr.execute(add_data_job, values_job)
                self.logger.info(
                    "  --------- JOB (" + item["job_id"] + ") IS STORED IN TABLE " + self.db_table_jobs + " --------- ")
            except Exception as e:
                self.logger.critical(
                    " --------- ERROR STORING ITEM IN DB JOBS --------- ")
                self.logger.critical(e)

            try:
                self.curr.execute(add_data_company, values_company)
                self.logger.info(
                    "  --------- COMPANY (" + item["company_id"] + ") IS STORED IN TABLE " + self.db_table_jobs + " --------- ")
            except Exception as e:
                self.logger.critical(
                    " --------- ERROR STORING ITEM IN DB COMPANY --------- ")
                self.logger.critical(e)

        self.conn.commit()
