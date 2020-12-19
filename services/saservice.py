from sqlalchemy import select, union
from sqlalchemy.sql import func, text


class SqlAlchemyService:

    table_name = None

    def __init__(self, connector):
        self.connector = connector
        self.metadata  = connector.metadata


    def conn(self):
        return self.connector.get_connection()


    def table(self, table_name=None):
        _table_name = table_name or self.table_name
        return self.metadata.tables.get(_table_name)


    def exec(self, stmt, *args):
        res = self.conn().execute(stmt, *args)
        return res


    def create(self, data):
        i = self.table().insert().values(**data)
        r = self.exec(i)
        _id = r.inserted_primary_key

        return _id[0] if len(_id) else None


    def create_many(self, items_list):
        i = self.table().insert()
        r = self.exec(i, items_list)


    def delete(self, entity_id):
        t = self.table()
        self.exec(t.delete().where(t.c.id == entity_id))


    def update(self, entity_id, data):
        t = self.table()
        q = t.update().values(**data).where(t.c.id == entity_id)

        return self.exec(q)


    def find(self, entity_id):
        t = self.table()
        s = t.select(t.c.id == entity_id)
        r = self.exec(s).fetchone()

        return dict(r) if r else None


    def find_by(self, col, val):
        t = self.table()
        s = t.select(t.c[col] == val)
        r = self.exec(s).fetchone()

        return dict(r) if r else None


    def find_all(self):
        r = self.exec(select([self.table()]))
        return r.fetchall() if r else []


    def paginate(self, sel=None, page=1, page_size=5):
        table     = self.table()
        items_sel = select([table]) if sel == None else sel
        offset    = ((page-1) * page_size) if page > 1 else 0

        limit_sel = items_sel\
                        .limit(page_size).offset(offset)\
                        .order_by(table.c.id)

        items_res = self.exec(limit_sel)
        items = [i for i in items_res.fetchall()] if items_res else []

        total = self._count_total_items(table, items_sel)

        return {
            'items': items,
            'total': total,
            'pages': self._calc_total_pages(total, page_size),
            'page':  page
        }


    def _count_total_items(self, table, items_sel):
        total_lb = 'total'

        # WARNING
        # _whereclause is a private property, that'll be added
        # in the 1.4 version of SqlAlchemy; it's used here to limit
        # the number of total results of a select query
        sel = select([func.count(table.c.id).label(total_lb)])\
                .where(items_sel._whereclause)

        res = self.exec(sel)
        return res.fetchone()[total_lb] if res else 0


    def _calc_total_pages(self, total, page_size):
        rest = int(total) % int(page_size)
        div  = int(total) // int(page_size)

        return div + 1 if rest > 0 else div


