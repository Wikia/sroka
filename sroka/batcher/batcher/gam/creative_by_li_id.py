import googleads.ad_manager as ad_manager
from ...gam_helpers import statement_iter_timer
from ..base import AbstractBatch


class CreativesByLineItemID(AbstractBatch):
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

        # Get creative id - line id association
        line_item_ids_string = [str(i["line_item_id"]) for i in self.container]
        search_statement = ad_manager.StatementBuilder().Where(
            f"lineItemId IN ({', '.join(line_item_ids_string)})"
        )
        lines_to_creatives = [
            (i["lineItemId"], i["creativeId"])
            for i in statement_iter_timer("LineItemCreativeAssociationService", search_statement)
        ]

        # Get creatives
        creative_ids_string = {str(i[1]) for i in lines_to_creatives}
        search_statement = ad_manager.StatementBuilder().Where(
            f"id IN ({', '.join(creative_ids_string)})"
        )
        creatives = {
            i["id"]: i
            for i in statement_iter_timer("CreativeService", search_statement)
        }

        # Match creatives and lines
        self.result = {}
        for line, creative in lines_to_creatives:
            if line not in self.result:
                self.result[line] = []
            self.result[line].append(creatives[creative])

    def get_value(self, kwargs):
        return self.result[int(kwargs["line_item_id"])]

    @classmethod
    def request(cls, line_item_id):
        return super().request(line_item_id=line_item_id)
