from edgar import *
from utils.log_utils import *


"""
This class is based on edgartools: https://github.com/dgunning/edgartools
"""


class EdgarDataLoader:
    def __init__(self):
        set_identity(EDGAR_IDENTITY)

    def fetch_latest_form_html(self, symbol: str, form_name: str="10-Q"):
        # Get the company
        company = Company(symbol)

        # Get the latest 10-Q filing
        filings = company.get_filings(form=form_name)
        if filings is None or len(filings) == 0:
            loge(f"No {form_name} filings found for {symbol}")
            return None

        latest_form = filings[0]
        return latest_form

    def extract_item_data(self, form_item: any, item_name: str="Item 2"):
        """
        Fetch and extract item data
        """
        # Convert into form object
        form_obj = form_item.obj()

        # Get item info
        try:
            item_text = form_obj[item_name]

            return item_text
        except Exception as ex:
            loge(f"Unable to get {item_name} from form data: {str(ex)}")
        return None


