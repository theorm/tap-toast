
#
# Module dependencies.
#

import os
import json
import datetime
import pytz
import singer
from singer import metadata
from singer import utils
from singer.metrics import Point
from dateutil.parser import parse
from tap_toast.context import Context


logger = singer.get_logger()
KEY_PROPERTIES = ['guid']


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def needs_parse_to_date(string):
    if isinstance(string, str):
        try:
            parse(string)
            return True
        except ValueError:
            return False
    return False


class Stream():
    name = None
    replication_method = None
    replication_key = None
    stream = None
    key_properties = KEY_PROPERTIES
    session_bookmark = None


    def __init__(self, client=None):
        self.client = client


    def get_bookmark(self, state):
        return (singer.get_bookmark(state, self.name, self.replication_key)) or Context.config["start_date"]


    def update_bookmark(self, state, value):
        if self.is_bookmark_old(state, value):
            singer.write_bookmark(state, self.name, self.replication_key, value)


    def is_bookmark_old(self, state, value):
        current_bookmark = self.get_bookmark(state)
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(current_bookmark)


    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return schema


    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)


    def is_selected(self):
        return self.stream is not None


    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        bookmark = self.get_bookmark(state)
        res = get_data(self.replication_key, bookmark)

        for item in res:
            if self.replication_method == "INCREMENTAL":
                self.update_bookmark(state, item[self.replication_key])
            yield (self.stream, item)


class CashManagementEntries(Stream):
    name = "cash_management_entries"
    replication_method = "INCREMENTAL"
    replication_key = "date"
    key_properties = [ "guid" ]


class CashManagementDeposits(Stream):
    name = "cash_management_deposits"
    replication_method = "INCREMENTAL"
    replication_key = "date"
    key_properties = [ "guid" ]


class Employees(Stream):
    name = "employees"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class Orders(Stream):
    name = "orders"
    replication_method = "INCREMENTAL"
    replication_key = "modifiedDate"
    key_properties = [ "guid" ]


class Payments(Stream):
    name = "payments"
    replication_method = "INCREMENTAL"
    replication_key = "paidDate"
    key_properties = [ "guid" ]


class AlternatePaymentTypes(Stream):
    name = "alternate_payment_types"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class BreakTypes(Stream):
    name = "break_types"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class CashDrawers(Stream):
    name = "cash_drawers"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class DiningOptions(Stream):
    name = "dining_options"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class Discounts(Stream):
    name = "discounts"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class MenuGroups(Stream):
    name = "menu_groups"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class MenuItems(Stream):
    name = "menu_items"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class MenuOptionGroups(Stream):
    name = "menu_option_groups"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class Menus(Stream):
    name = "menus"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class NoSaleReasons(Stream):
    name = "no_sale_reasons"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class PayoutReasons(Stream):
    name = "payout_reasons"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class PreModifierGroups(Stream):
    name = "premodifier_groups"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class PreModifiers(Stream):
    name = "premodifiers"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class PriceGroups(Stream):
    name = "price_groups"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class Printers(Stream):
    name = "printers"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class RestaurantServices(Stream):
    name = "restaurant_services"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class RevenueCenters(Stream):
    name = "revenue_centers"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class SalesCategories(Stream):
    name = "sales_categories"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class ServiceAreas(Stream):
    name = "service_areas"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class Tables(Stream):
    name = "tables"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class TaxRates(Stream):
    name = "tax_rates"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class TipWithholding(Stream):
    name = "tip_withholding"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class VoidReasons(Stream):
    name = "void_reasons"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]


class Restaurants(Stream):
    name = "restaurants"
    replication_method = "FULL_TABLE"
    key_properties = [ "guid" ]



STREAMS = {
    "cash_management_entries": CashManagementEntries,
    "cash_management_deposits": CashManagementDeposits,
    "employees": Employees,
    "orders": Orders,
    "payments": Payments,
    "alternate_payment_types": AlternatePaymentTypes,
    "break_types": BreakTypes,
    "cash_drawers": CashDrawers,
    "dining_options": DiningOptions,
    "discounts": Discounts,
    "menu_groups": MenuGroups,
    "menu_items": MenuItems,
    "menu_option_groups": MenuOptionGroups,
    "menus": Menus,
    "no_sale_reasons": NoSaleReasons,
    "payout_reasons": PayoutReasons,
    "premodifier_groups": PreModifierGroups,
    "premodifiers": PreModifiers,
    "price_groups": PriceGroups,
    "printers": Printers,
    "restaurant_services": RestaurantServices,
    "revenue_centers": RevenueCenters,
    "sales_categories": SalesCategories,
    "service_areas": ServiceAreas,
    "tables": Tables,
    "tax_rates": TaxRates,
    "tip_withholding": TipWithholding,
    "void_reasons": VoidReasons,
    "restaurants": Restaurants
}



