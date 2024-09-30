from ..batcher import Receipt, LineItemsById, CreativesByLineItemID

class AdProductExecution:
    def __init__(self, line_item_ids: list[int]):
        self._line_items = [LineItemsById.request(i) for i in line_item_ids]
        self._creatives = {i: CreativesByLineItemID.request(i) for i in line_item_ids}
        # self.deal_line_id = deal_line_id

    @property
    def line_items(self) -> list:
        return [i.get() for i in self._line_items]

    @property
    def creatives(self):
        return sum((i.get() for i in self._creatives.values()), [])

    def creatives_of(self, line_id):
        return self._creatives[line_id].get()
