from sqlalchemy import select
from sqlalchemy.sql import func


class SqlAlchemyService:

    table_name = None

    def __init__(self, connector):
        self.connector = connector
        self.metadata  = connector.metadata
        self.conn      = connector.conn


    def table(self, table_name=None):
        _table_name = table_name or self.table_name
        return self.metadata.tables.get(_table_name)


    def exec(self, stmt, *args):
        res = self.conn.execute(stmt, *args)
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
        r = self.exec(s)

        return r.fetchall()


    def find_all(self):
        r = self.exec(select([self.table()]))
        return r.fetchall() if r else []


    def paginate(self, sel=None, page=1, page_size=5):
        total_lb  = 'total'
        count_sel = select([func.count(self.table().c.id).label(total_lb)])
        items_sel = select([self.table()])

        offset = ((page-1) * page_size) if page > 1 else 0

        sel_ = select([count_sel.label(total_lb), items_sel])\
                    .limit(page_size).offset(offset) if sel == None else sel

        r = self.exec(sel_)

        def map_fn(el):
            el.pop(total_lb)
            return el

        res_list = [dict(i) for i in r.fetchall()] if r else []

        # total number of items
        total = res_list[0][total_lb]

        query_res = list(map(map_fn, res_list))

        return {
            'items': query_res,
            'total': total,
            'pages': self._calc_total_pages(total, page_size),
            'page':  page
        }


    def _calc_total_pages(self, total, page_size):
        rest = int(total) % int(page_size)
        div  = int(total) // int(page_size)

        return div + 1 if rest > 0 else div


