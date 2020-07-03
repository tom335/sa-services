from sqlalchemy import create_engine, MetaData

class SqlAlchemyConnector():

    def __init__(self, db_path, echo=True):
        self.engine = create_engine(db_path, echo=echo)
        self.conn = self.engine.connect()
        self.metadata = MetaData(bind=self.engine)

        self.metadata.reflect(self.engine)

