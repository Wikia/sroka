class WeirdGEOException(Exception):
    pass

class CannotAddToBuiltBatchException(Exception):
    def __init__(self, batch) -> None:
        self.batch = batch
        mess = f"You cannot add new requests to a batcher that is '{batch.status}'"
        super().__init__(mess)

class DataNotFoundException(Exception):

    @classmethod
    def creative_template(cls, id):
        return cls(f"Creative template '{id}' doesn't exist in GAM")
