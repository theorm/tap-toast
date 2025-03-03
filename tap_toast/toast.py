
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
import json

logger = logging.getLogger()
utc = pytz.UTC



def get_start_end_hour(start_date, end_date):
    delta = timedelta(hours=1)
    format_string = '%Y-%m-%dT%H:%M:%S.000-0000' # hard coding this timezone because it's too complicated
    while start_date < end_date:
        yield (start_date.strftime(format_string), (start_date + delta).strftime(format_string))
        start_date += delta



def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)



class Toast(object):

    def __init__(self, client_id=None, client_secret=None, location_guid=None, management_group_guid=None, start_date=None, auth_with_login=True):
        """ Simple Python wrapper for the Toast API. """
        self.host = 'https://ws-api.toasttab.com/'
        self.client_id = client_id
        self.client_secret = client_secret
        self.location_guid = location_guid
        self.management_group_guid = management_group_guid
        self.start_date = utils.strptime_with_tz(start_date)
        self.grant_type = 'client_credentials'
        self.user_access_type = 'TOAST_MACHINE_CLIENT'
        self.auth_with_login = auth_with_login
        self.authorization_token = None
        self.fmt_date_time = '%Y-%m-%dT%H:%M:%S.%Z'
        self.fmt_date = '%Y%m%d'
        self.default_page_size = 50
        self.get_authorization_token()
        # print(self.authorization_token)


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
        try:
            res = response.json()
            if isinstance(res, dict):
                res = [res]
        except ValueError:
            res = []
        return res


    def is_authorized(self):
        return self.authorization_token is not None


    def get_authorization_token(self):
        if self.auth_with_login:
            return self.get_authorization_token_with_login()
        payload = { 'grant_type': self.grant_type, 'client_id': self.client_id, 'client_secret': self.client_secret }
        response = requests.post(self._url('usermgmt/v1/oauth/token'), data=payload)
        response.raise_for_status()
        res = response.json()
        logger.info('Authorization successful.')
        self.authorization_token = res['access_token']

    def get_authorization_token_with_login(self):
        payload = { 'userAccessType': self.user_access_type, 'clientId': self.client_id, 'clientSecret': self.client_secret }
        response = requests.post(self._url('authentication/v1/authentication/login'), json=payload, headers={ 'Content-Type': 'application/json' })
        response.raise_for_status()
        res = response.json()
        logger.info('Authorization successful.')
        self.authorization_token = res['token']['accessToken']


    # column_name, bookmark
    def cash_management_entries(self, column_name=None, bookmark=None):
        business_date = utils.strptime_with_tz(bookmark).strftime(self.fmt_date)
        for single_date in daterange(utils.strptime_with_tz(business_date), datetime.now(pytz.utc)):
            logger.info('Hitting cash management entries endpoint at datetime {date}'.format(date=single_date))
            res = self._get(self._url('cashmgmt/v1/entries'), businessDate=single_date.strftime(self.fmt_date))
            logger.info('Returned {number} entries.'.format(number=len(res)))
            for item in res:
                yield item


    # column_name, bookmark
    def cash_management_deposits(self, column_name=None, bookmark=None):
        business_date = utils.strptime_with_tz(bookmark).strftime(self.fmt_date)
        for single_date in daterange(utils.strptime_with_tz(business_date), datetime.now(pytz.utc)):
            logger.info('Hitting cash management deposits endpoint at date {date}'.format(date=single_date))
            res = self._get(self._url('cashmgmt/v1/deposits'), businessDate=single_date.strftime(self.fmt_date))
            logger.info('Returned {number} deposits.'.format(number=len(res)))
            for item in res:
                yield item


    # full table sync
    def employees(self, column_name=None, bookmark=None):
        res = self._get(self._url('labor/v1/employees'))
        for item in res:
            yield item


    def orders(self, column_name=None, bookmark=None):
        business_date = utils.strptime_with_tz(bookmark).strftime(self.fmt_date_time)
        
        format_string = '%Y-%m-%dT%H:%M:%S.000Z'
        start_datetime = utils.strptime_with_tz(business_date).strftime(format_string)
        end_datetime = datetime.now(pytz.utc).strftime(format_string)
        page = 1
        has_more = True
        while has_more:
            logger.info(f'Hitting orders endpoint between date {start_datetime} and {end_datetime} and page {page}')
            res = self._get(self._url('orders/v2/ordersBulk'), startDate=start_datetime, endDate=end_datetime, page=page, pageSize=100)
            for item in res:
                yield item
            has_more = len(res) > 0
            page += 1


    def payments(self, column_name=None, bookmark=None):
        # cycle through paidBusinessDate, refundBusinessDate, and voidBusinessDate
        business_date = utils.strptime_with_tz(bookmark).strftime(self.fmt_date)
        for single_date in daterange(utils.strptime_with_tz(business_date), datetime.now(pytz.utc)):
            logger.info('Hitting endpoint at date {date}'.format(date=single_date))
            paid_res = self._get(self._url('orders/v2/payments'), paidBusinessDate=single_date.strftime(self.fmt_date))
            refund_res = self._get(self._url('orders/v2/payments'), refundBusinessDate=single_date.strftime(self.fmt_date))
            void_res = self._get(self._url('orders/v2/payments'), voidBusinessDate=single_date.strftime(self.fmt_date))
            res = paid_res + refund_res + void_res
            logger.info('Returned {number} payments.'.format(number=len(res)))
            for item in res:
                yield self._get(self._url('orders/v2/payments/{payment_guid}'.format(payment_guid=item)))[0]


    def alternate_payment_types(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/alternatePaymentTypes'))
        for item in res:
            yield item


    def break_types(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/breakTypes'))
        for item in res:
            yield item


    def cash_drawers(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/cashDrawers'))
        for item in res:
            yield item


    def dining_options(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/diningOptions'))
        for item in res:
            yield item


    def discounts(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/discounts'))
        for item in res:
            yield item


    def menu_groups(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/menuGroups')) # pageSize not supported anymore
        for item in res:
            yield item


    def menu_items(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/menuItems')) # pageSize not supported anymore
        for item in res:
            yield item


    def menu_option_groups(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/menuOptionGroups'))
        for item in res:
            yield item


    def menus(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/menus'))
        for item in res:
            yield item


    def no_sale_reasons(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/noSaleReasons'))
        for item in res:
            yield item


    def payout_reasons(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/payoutReasons'))
        for item in res:
            yield item


    def premodifier_groups(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/preModifierGroups'))
        for item in res:
            yield item


    def premodifiers(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/preModifiers'))
        for item in res:
            yield item


    def price_groups(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/priceGroups'))
        for item in res:
            yield item


    def printers(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/printers'))
        for item in res:
            yield item


    def restaurant_services(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/restaurantServices'))
        for item in res:
            yield item


    def revenue_centers(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/revenueCenters'))
        for item in res:
            yield item


    def sales_categories(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/salesCategories'))
        for item in res:
            yield item


    def service_areas(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/serviceAreas'))
        for item in res:
            yield item


    def tables(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/tables'))
        for item in res:
            yield item


    def tax_rates(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/taxRates'))
        for item in res:
            yield item


    def tip_withholding(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/tipWithholding'))
        for item in res:
            yield item


    def void_reasons(self, column_name=None, bookmark=None):
        res = self._get(self._url('config/v2/voidReasons'))
        for item in res:
            yield item


    def restaurants(self, column_name=None, bookmark=None):
        restaurant_ids = self._get(self._url('restaurants/v1/groups/{management_group_guid}/restaurants'.format(management_group_guid=self.management_group_guid)))
        for restaurant_id in restaurant_ids:
            restaurants = self._get(self._url('restaurants/v1/restaurants/{restaurant_guid}'.format(restaurant_guid=restaurant_id["guid"])))
            for restaurant in restaurants:
                yield restaurant


