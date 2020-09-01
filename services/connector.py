from sqlalchemy import create_engine, MetaData


class SqlAlchemyConnector():

    def __init__(self, db_path, echo=True):
        self.engine = create_engine(db_path, echo=echo)
                                    # connect_args={'check_same_thread': False})
        self.metadata = MetaData(bind=self.engine)

    def get_connection(self):
        return self.engine.connect()



