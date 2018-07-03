import logging

from dynamo.source.replicainfo import ReplicaInfoSource
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.utils.parallel import Map
from dynamo.dataformat import Group, Site, Dataset, Block, File
from dynamo.dataformat import DatasetReplica, BlockReplica, Configuration

LOG = logging.getLogger(__name__)

class PhEDExReplicaInfoSource(ReplicaInfoSource):
    """ReplicaInfoSource using PhEDEx."""

    def __init__(self, config = None):
        if config is None:
            config = Configuration()

        ReplicaInfoSource.__init__(self, config)

        self._phedex = PhEDEx(config.get('phedex', None))

    def replica_exists_at_site(self, site, item): #override
        options = ['node=' + site.name]
        if type(item) == Dataset:
            options += ['dataset=' + item.name, 'show_dataset=y']
        elif type(item) == DatasetReplica:
            options += ['dataset=' + item.dataset.name, 'show_dataset=y']
        elif type(item) == Block:
            options += ['block=' + item.full_name()]
        elif type(item) == BlockReplica:
            options += ['block=' + item.block.full_name()]
        else:
            raise RuntimeError('Invalid input passed: ' + repr(item))
        
        source = self._phedex.make_request('blockreplicas', options, timeout = 600)

        if len(source) != 0:
            return True

        options = ['node=' + site.name]
        if type(item) == Dataset:
            # check both dataset-level and block-level subscriptions
            options += ['dataset=' + item.name, 'block=%s#*' % item.name]
        elif type(item) == DatasetReplica:
            options += ['dataset=' + item.dataset.name, 'block=%s#*' % item.dataset.name]
        elif type(item) == Block:
            options += ['block=' + item.full_name()]
        elif type(item) == BlockReplica:
            options += ['block=' + item.block.full_name()]

        # blockreplicas has max ~20 minutes latency
        source = self._phedex.make_request('subscriptions', options, timeout = 600)

        return len(source) != 0

    def get_replicas(self, site = None, dataset = None, block = None): #override
        if site is None:
            site_check = self.check_allowed_site
        else:
            site_check = None
            if not self.check_allowed_site(site):
                return []

        if dataset is None and block is None:
            dataset_check = self.check_allowed_dataset
        else:
            dataset_check = None
            if dataset is not None:
                if not self.check_allowed_dataset(dataset):
                    return []
            if block is not None:
                if not self.check_allowed_dataset(block[:block.find('#')]):
                    return []

        options = []
        if site is not None:
            options.append('node=' + site)
        if dataset is not None:
            options.append('dataset=' + dataset)
        if block is not None:
            options.append('block=' + block)

        LOG.info('get_replicas(' + ','.join(options) + ')  Fetching the list of replicas from PhEDEx')

        if len(options) == 0:
            return []
        
        result = self._phedex.make_request('filereplicas', options, timeout = 3600)

        block_replicas = PhEDExReplicaInfoSource.make_block_replicas(result, PhEDExReplicaInfoSource.maker_filereplicas, site_check = site_check, dataset_check = dataset_check)
        
        # Also use subscriptions call which has a lower latency than blockreplicas
        # For example, group change on a block replica at time T may not show up in blockreplicas until up to T + 15 minutes
        # while in subscriptions it is visible within a few seconds
        # But subscriptions call without a dataset or block takes too long
        if dataset is None and block is None:
            return block_replicas

        result = self._phedex.make_request('subscriptions', options, timeout = 3600)

        for dataset_entry in result:
            dataset_name = dataset_entry['name']

            if not self.check_allowed_dataset(dataset_name):
                continue

            try:
                subscriptions = dataset_entry['subscription']
            except KeyError:
                pass
            else:
                for sub_entry in subscriptions:
                    site_name = sub_entry['node']

                    if not self.check_allowed_site(site_name):
                        continue

                    for replica in block_replicas:
                        if replica.block.dataset.name == dataset_name and replica.site.name == site_name:
                            replica.group = Group(sub_entry['group'])
                            replica.is_custodial = (sub_entry['custodial'] == 'y')

            try:
                block_entries = dataset_entry['block']
            except KeyError:
                pass
            else:
                for block_entry in block_entries:
                    _, block_name = Block.from_full_name(block_entry['name'])

                    try:
                        subscriptions = block_entry['subscription']
                    except KeyError:
                        pass
                    else:
                        for sub_entry in subscriptions:
                            site_name = sub_entry['node']

                            if not self.check_allowed_site(site_name):
                                continue

                            for replica in block_replicas:
                                if replica.block.dataset.name == dataset_name and \
                                        replica.block.name == block_name and \
                                        replica.site.name == site_name:

                                    replica.group = Group(sub_entry['group'])
                                    if sub_entry['node_bytes'] == block_entry['bytes']:
                                        # complete
                                        replica.files = None
                                    else:
                                        # should in principle set to the list of file ids, but CMS instance does not use file-level information at the moment
                                        replica.files = tuple()
                                    replica.is_custodial = (sub_entry['custodial'] == 'y')
                                    replica.size = sub_entry['node_bytes']
                                    if sub_entry['time_update'] is not None:
                                        replica.last_update = 0
                                    else:
                                        replica.last_update = int(sub_entry['time_update'])

        return block_replicas

    def get_updated_replicas(self, updated_since): #override
        LOG.info('get_updated_replicas(%d)  Fetching the list of replicas from PhEDEx', updated_since)

        nodes = []
        for entry in self._phedex.make_request('nodes', timeout = 600):
            if not self.check_allowed_site(entry['name']):
                continue

            nodes.append(entry['name'])

        parallelizer = Map()
        parallelizer.timeout = 7200

        # Use async to fire threads on demand
        arguments = [('blockreplicas', ['update_since=%d' % updated_since, 'node=%s' % node]) for node in nodes]
        block_replicas = parallelizer.execute(self._phedex.make_request, arguments, callback = PhEDExReplicaInfoSource.combine_file_info, async = True)

        def make_block_entry_with_files(block_entry):
            try:
                new_block_entry = self._phedex.make_request('filereplicas', ['block=%s' % block_entry['name'], 'node=%s' % block_entry['replica'][0]['node']])[0]
            except IndexError:
                # Somehow PhEDEx didn't have a filereplicas entry for this block at this node
                new_block_entry = block_entry
                new_block_entry['file'] = []
            else:
                new_block_entry['replica'] = block_entry['replica']

            return new_block_entry
            
        # Automatically starts a thread as we add the output of block_replicas
        entry_maker = parallelizer.get_starter(make_block_entry_with_files)

        for block_entry in block_replicas:
            entry_maker.add_input(block_entry)

        return PhEDExReplicaInfoSource.make_block_replicas(entry_maker.get_outputs(), PhEDExReplicaInfoSource.maker_filereplicas, dataset_check = self.check_allowed_dataset)

    def get_deleted_replicas(self, deleted_since): #override
        LOG.info('get_deleted_replicas(%d)  Fetching the list of replicas from PhEDEx', deleted_since)

        result = self._phedex.make_request('deletions', ['complete_since=%d' % deleted_since], timeout = 7200)

        return PhEDExReplicaInfoSource.make_block_replicas(result, PhEDExReplicaInfoSource.maker_deletions)

    @staticmethod
    def make_block_replicas(block_entries, replica_maker, site_check = None, dataset_check = None):
        """Return a list of block replicas linked to Dataset, Block, Site, and Group"""

        dataset = None
        block_replicas = []

        for block_entry in block_entries:
            try:
                dataset_name, block_name = Block.from_full_name(block_entry['name'])
            except ValueError: # invalid name
                continue

            if dataset is None or dataset.name != dataset_name:
                if dataset_check and not dataset_check(dataset_name):
                    continue
    
                dataset = Dataset(
                    dataset_name
                )
            
            block = Block(
                block_name,
                dataset,
                block_entry['bytes']
            )

            block_replicas.extend(replica_maker(block, block_entry, site_check = site_check))

        return block_replicas

    @staticmethod
    def maker_filereplicas(block, block_entry, site_check = None):
        """Return a list of block replicas using filereplicas data or a combination of blockreplicas and filereplicas calls."""

        block.files = set()

        block_replicas = {}
        groups = {}
        invalid_sites = set()

        for file_entry in block_entry['file']:
            lfile = File(file_entry['name'], block, size = file_entry['bytes'])
            block.files.add(lfile)
            
            for replica_entry in file_entry['replica']:
                site_name = replica_entry['node']
                try:
                    block_replica = block_replicas[site_name]
                except KeyError:
                    if site_name in invalid_sites:
                        continue

                    if site_check and not site_check(site_name):
                        invalid_sites.add(site_name)
                        continue

                    site = Site(site_name)

                    group_name = replica_entry['group']
                    try:
                        group = groups[group_name]
                    except KeyError:
                        group = Group(group_name)

                    block_replica = block_replicas[site_name] = BlockReplica(
                        block,
                        site,
                        group,
                        is_custodial = (replica_entry['custodial'] == 'y'),
                        last_update = 0
                    )
                    # temporarily make this a list
                    block_replica.file_ids = []

                try:
                    time_create = int(replica_entry['time_create'])
                except TypeError:
                    pass
                else:
                    if time_create > block_replica.last_update:
                        block_replica.last_update = time_create

                block_replica.file_ids.append(file_entry['name'])
                block_replica.size += lfile.size

        for block_replica in block_replicas.itervalues():
            block_replica.file_ids = tuple(block_replica.file_ids)

        # if combined source was given, add some more information
        if 'replica' in block_entry:
            for replica_entry in block_entry['replica']:
                try:
                    block_replica = block_replicas[site_name]
                except KeyError:
                    if site_name in invalid_sites:
                        continue

                    if site_check and not site_check(site_name):
                        invalid_sites.add(site_name)
                        continue

                    site = Site(site_name)

                    group_name = replica_entry['group']
                    try:
                        group = groups[group_name]
                    except KeyError:
                        group = Group(group_name)

                    block_replica = block_replicas[site_name] = BlockReplica(
                        block,
                        site,
                        group,
                        is_custodial = (replica_entry['custodial'] == 'y'),
                        size = 0,
                        last_update = 0
                    )

                try:
                    time_update = int(replica_entry['time_update'])
                except TypeError:
                    pass
                else:
                    if time_update > block_replica.last_update:
                        block_replica.last_update = time_update
               
        return block_replicas.values()

    @staticmethod
    def maker_deletions(block, block_entry, site_check = None):
        replicas = []

        for deletion_entry in block_entry['deletion']:
            if site_check and not site_check(deletion_entry['node']):
                continue

            block_replica = BlockReplica(block, Site(deletion_entry['node']), Group.null_group)

            replicas.append(block_replica)

        return replicas
