
#
# Module dependencies.
#

from requests.auth import HTTPBasicAuth
from datetime import date, datetime, timedelta, timezone
from singer import utils
import backoff
import requests
import logging
import pytz
import sys

logger = logging.getLogger()
utc = pytz.UTC



def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)



class Toast(object):


    def __init__(self, client_id=None, client_secret=None, location_guid=None, start_date=None):
        """ Simple Python wrapper for the Toast API. """
        self.host = 'https://ws-sandbox-api.eng.toasttab.com/'
        self.client_id = client_id
        self.client_secret = client_secret
        self.location_guid = location_guid
        self.start_date = utils.strptime_with_tz(start_date)
        self.grant_type = 'client_credentials'
        self.authorization_token = None
        self.fmt_date = '%Y%m%d'
        self.get_authorization_token()


    def _url(self, path):
        return self.host + path


    @backoff.on_exception(backoff.expo,
                        requests.exceptions.RequestException)
    def _post(self, url, **kwargs):
        if self.authorization_token is None:
            self.get_authorization_token()

        header = { 'Authorization': 'Bearer ' + self.authorization_token, 'Toast-Restaurant-External-ID': self.location_guid, 'Content-Type': 'application/json' }
        response = requests.post(url, headers=header)
        response.raise_for_status()
        logger.info('POST request successful at {url}'.format(url=url))
        return response.json()


    @backoff.on_exception(backoff.expo,
                        requests.exceptions.RequestException)
    def _get(self, url, **kwargs):
        if self.authorization_token is None:
            self.get_authorization_token()

        header = { 'Authorization': 'Bearer ' + self.authorization_token, 'Toast-Restaurant-External-ID': self.location_guid, 'Content-Type': 'application/json' }
        response = requests.get(url, headers=header, params=kwargs)
        response.raise_for_status()
        logger.info('GET request successful at {url}'.format(url=url))
        return response.json()


    def is_authorized(self):
        return self.authorization_token is not None


    def get_authorization_token(self):
        payload = { 'grant_type': self.grant_type, 'client_id': self.client_id, 'client_secret': self.client_secret }
        response = requests.post(self._url('usermgmt/v1/oauth/token'), data=payload)
        response.raise_for_status()
        res = response.json()
        logger.info('Authorization successful.')
        self.authorization_token = res['access_token']


    # column_name, bookmark
    # def cash_management_entries(self, business_date):
    #     res = self._get(self._url('cashmgmt/v1/entries'), businessDate=business_date)
    #     for item in res:
    #         yield res


    # column_name, bookmark
    # def cash_management_deposits(self, business_date):
    #     res = self._get(self._url('cashmgmt/v1/deposits'), businessDate=business_date) 
    #     for item in res:
    #         yield res


    # full table sync
    # def employees(self):
    #     res = self._get(self._url('employees'))
    #     for item in res:
    #         yield res


    def orders(self, column_name=None, bookmark=None):
        business_date = (self.start_date).strftime(self.fmt_date)
        if bookmark is not None:
            business_date = utils.strptime_with_tz(bookmark).strftime(self.fmt_date)

        for single_date in daterange(utils.strptime_with_tz(business_date), datetime.now(pytz.utc)):
            logger.info('Hitting endpoint at date {date}'.format(date=single_date))
            res = self._get(self._url('orders/v2/orders/'), businessDate=single_date.strftime(self.fmt_date))
            logger.info('Returned {number} orders.'.format(number=len(res)))
            for item in res:
                order = self._get(self._url('orders/v2/orders/{order_guid}'.format(order_guid=item)))
                yield order


    # def payments(self, column_name=None, bookmark=None):

    #     res = self._get(self._url('payments'), paidBusinessDate=business_date)
    #     for item in res:
    #         payment = self._get(self._url('payments/{payment_guid}'.format(payment_guid=item)))
    #         yield payment


