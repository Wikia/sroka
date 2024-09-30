import googleads.ad_manager as ad_manager
from ...gam_helpers import statement_iter_timer
from ..base import AbstractBatch


class OrdersById(AbstractBatch):
    """
    A class for retrieving GAM line items by their IDs.
    """

    def __init__(self):
        super().__init__()
        self.result = None

    def limit(self):
        return 50

    def fetch(self):
        """Retrieves line items"""
        line_item_ids_string = [str(i["id_"]) for i in self.container]
        search_statement = ad_manager.StatementBuilder().Where(
            f"Id IN ({', '.join(line_item_ids_string)})"
        )
        self.result = {
            i["id"]: i
            for i in statement_iter_timer("OrderService", search_statement)
        }

    def get_value(self, kwargs):
        return self.result[int(kwargs["id_"])]

    @classmethod
    def request(cls, id_):
        return super().request(id_=id_)
