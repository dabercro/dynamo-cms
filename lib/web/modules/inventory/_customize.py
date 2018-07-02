import collections
from dynamo.dataformat import Dataset, Site, Group

def campaign_name(dataset):
    sd = dataset.name[dataset.name.find('/', 1) + 1:dataset.name.rfind('/')]
    return sd[:sd.find('-')]

def customize_stats(categories):
    categories.categories = collections.OrderedDict([
        ('campaign', ('Production campaign', Dataset, campaign_name)),
        ('data_tier', ('Data tier', Dataset, lambda d: d.name[d.name.rfind('/') + 1:])),
        ('dataset_status', ('Dataset status', Dataset, lambda d: Dataset.status_name(d.status))),
        ('dataset', ('Dataset name', Dataset, lambda d: d.name)),
        ('site', ('Site name', Site, lambda s: s.name)),
        ('group', ('Group name', Group, lambda g: g.name))
    ])
