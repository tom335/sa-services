
from sqlalchemy import Table


class Services:

    def __init__(self):
        self.services = {}

    def add(self, service_name, service_instance):
        self.services[service_name] = service_instance

    def get(self, service_name):
        return self.services[service_name]


class ServiceFactory:

    @staticmethod
    def create_service(service_name, service_class, connector):
        global services
        services.add(service_name, service_class(connector))


def create_tables(tables, connector):

    for n in tables.keys():
        Table(n, connector.metadata, *tables[n])

    connector.metadata.create_all(connector.engine)


def init_services(services_map, connector):
    for s_key, s_class in services_map.items():
        s_name = s_key + '_service'
        ServiceFactory.create_service(s_name, s_class, connector)


services = Services()
