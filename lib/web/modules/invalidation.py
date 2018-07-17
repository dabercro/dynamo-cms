from dynamo.web.modules._base import WebModule
from dynamo.web.exceptions import MissingParameter, ExtraParameter, AuthorizationError
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.utils.interface.dbs import DBS
from dynamo.registry.registry import RegistryDatabase
from dynamo.dataformat import Block

class InvalidationRequest(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)

        self.dbs = DBS()
        self.phedex = PhEDEx()
        self.registry = RegistryDatabase()
        self.authorized_users = list(config.file_invalidation.authorized_users)

    def run(self, caller, request, inventory):
        if caller.name not in self.authorized_users:
            raise AuthorizationError()

        try:
            item = request['item']
        except KeyError:
            raise MissingParameter('item')

        if type(item) is list:
            items = item
        else:
            items = [item]

        invalidated_items = []

        sql = 'INSERT INTO `invalidations` (`item`, `db`, `user_id`, `timestamp`) VALUES (%s, %s, %s, NOW())'

        for item in items:
            invalidated = False

            if item in inventory.datasets:
                # item is a dataset
    
                result = self.dbs.make_request('datasets', ['dataset=' + item, 'dataset_access_type=*', 'detail=true'])
                if len(result) != 0:
                    status = result[0]['dataset_access_type']
                    if status in ('VALID', 'PRODUCTION'):
                        self.registry.db.query(sql, item, 'dbs', caller.id)
    
                    for entry in self.dbs.make_request('files', ['dataset=' + item, 'validFileOnly=1']):
                        self.registry.db.query(sql, entry['logical_file_name'], 'dbs', caller.id)

                    invalidated = True
    
                result = self.phedex.make_request('data', ['dataset=' + item, 'level=block'])
                if len(result) != 0:
                    self.registry.db.query(sql, item, 'tmdb', caller.id)
                    invalidated = True

            else:
                try:
                    dataset_name, block_name = Block.from_full_name(item)
                except:
                    lfile = inventory.find_file(item)
                    if lfile is not None:
                        # item is a file
                        
                        result = self.dbs.make_request('files', ['logical_file_name=' + item, 'validFileOnly=1'])
                        if len(result) != 0:
                            self.registry.db.query(sql, result[0]['logical_file_name'], 'dbs', caller.id)
                            invalidated = True
    
                        result = self.phedex.make_request('data', ['file=' + item])
                        if len(result) != 0:
                            self.registry.db.query(sql, item, 'tmdb', caller.id)
                            invalidated = True

                else:
                    # item is a block
        
                    for entry in self.dbs.make_request('files', ['block_name=' + item, 'validFileOnly=1']):
                        self.registry.db.query(sql, entry['logical_file_name'], 'dbs', caller.id)
                        invalidated = True
        
                    result = self.phedex.make_request('data', ['block=' + item, 'level=block'])
                    if len(result) != 0:
                        self.registry.db.query(sql, item, 'tmdb', caller.id)
                        invalidated = True
    
            if invalidated:
                invalidated_items.append({'item': item})

        return invalidated_items


class InvalidationCancel(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)

        self.dbs = DBS()
        self.registry = RegistryDatabase()
        self.authorized_users = list(config.file_invalidation.authorized_users)

    def run(self, caller, request, inventory):
        if caller.name not in self.authorized_users:
            raise AuthorizationError()

        try:
            item = request['item']
        except KeyError:
            raise MissingParameter('item')

        if type(item) is list:
            items = item
        else:
            items = [item]

        cancelled_items = []

        sql = 'DELETE FROM `invalidations` WHERE `item` = %s AND `user_id` = %s'

        for item in items:
            deleted = self.registry.db.query(sql, item, caller.id)
            if deleted != 0:
                cancelled_items.append({'item': item})

            if item in inventory.datasets:
                # item is a dataset
    
                for entry in self.dbs.make_request('files', ['dataset=' + item, 'validFileOnly=1']):
                    self.registry.db.query(sql, entry['logical_file_name'], caller.id)

            else:
                try:
                    dataset_name, block_name = Block.from_full_name(item)
                except:
                    pass
                else:
                    # item is a block

                    for entry in self.dbs.make_request('files', ['block_name=' + item, 'validFileOnly=1']):
                        self.registry.db.query(sql, entry['logical_file_name'], caller.id)

        return cancelled_items


export_data = {
    'invalidate': InvalidationRequest,
    'clear': InvalidationCancel
}

export_web = {}

# backward compatibility
registry_alias = {
    'invalidation': export_data
}
