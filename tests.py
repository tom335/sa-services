from sqlalchemy.sql import select, and_
from services import SqlAlchemyConnector, SqlAlchemyService
from schema import create_tables



############################################
# Services definitions
############################################


class User(SqlAlchemyService):

    table_name = 'user'


class Address(SqlAlchemyService):

    table_name = 'address'


class Post(SqlAlchemyService):


    table_name = 'post'


    def all_with_categories(self):
        posts = self.table()
        categories = self.table('category')
        posts_categories = self.table('post_category')

        # TODO test a group concat for categories

        s = select([posts, categories.c.label])\
                .where(
                    and_(posts.c.id == posts_categories.c.post_id,
                         categories.c.id == posts_categories.c.category_id)
                )

        s1 = s.limit(1).offset(1)

        return self.exec(s1).fetchall()


    def add_post_category(self, post_id, category_id):
        pc_table = self.table('post_category')

        ins = pc_table.insert().values({'post_id': post_id, 'category_id': category_id})

        self.exec(ins)



class Category(SqlAlchemyService):

    table_name = 'category'



############################################
# Test functions
############################################



def test_user_crud(user_service):
    # create object
    uid = user_service.create({'first_name': 'Toulouds', 'last_name': 'Clapton'})
    print('uid', uid)

    # read, or find the object
    user = user_service.find(uid)
    print(user.get('first_name'), user.get('last_name'))

    # update user
    user_service.update(uid, {'first_name': 'Tilquilds'})

    user = user_service.find(uid)
    print(user.get('first_name'), user.get('last_name'))

    # finally, delete the poor tilquilds
    user_service.delete(uid)

    user = user_service.find(uid)
    print(user)


def test_insert_multiple(user_service):

    users = [
        {'first_name': 'User 1', 'last_name': 'Surname'},
        {'first_name': 'User 2', 'last_name': 'Surname'},
        {'first_name': 'User 3', 'last_name': 'Surname'},
        {'first_name': 'User 4', 'last_name': 'Surname'},
    ]

    user_service.create_many(users)

    u2 = user_service.find_by('first_name', 'User 2')
    u4 = user_service.find_by('first_name', 'User 4')

    print (u2)
    print (u4)


def test_create_categories(service):
    categories  = list(map(lambda c : { 'label': c }, [
        'boats', 'houses', 'cars', 'electronics', 'cleaning', 'house & kitchen'
    ]))

    service.create_many(categories)



def test_create_posts(service):
    posts = [{
        'title': f'Title {i}',
        'content': f'Content {i}'
    } for i in range(1, 10)]

    service.create_many(posts)

    service.add_post_category(1, 4)
    service.add_post_category(3, 2)
    service.add_post_category(4, 5)




if __name__ == '__main__':
    db_path = 'sqlite:///:memory:'
    connector = SqlAlchemyConnector(db_path, False)

    create_tables(connector)

    # instantiate services
    user_service = User(connector)
    post_service = Post(connector)
    cat_service  = Category(connector)

    # test user services
    test_user_crud(user_service)
    test_insert_multiple(user_service)

    # test category services
    test_create_categories(cat_service)

    # test posts
    test_create_posts(post_service)

    # test find_all
    post_service.find_all()

    # test find post
    print (post_service.find(1))
    print (post_service.find(3))
    print (post_service.find(4))


    print (post_service.all_with_categories())


    print ('Pagination tests')
    print (post_service.paginate(page=2, page_size=3))


