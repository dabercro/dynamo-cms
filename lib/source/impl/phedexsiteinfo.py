import time
import logging
import threading
import re

from dynamo.dataformat import Configuration, Site
from dynamo.source.siteinfo import SiteInfoSource
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.utils.interface.ssb import SiteStatusBoard

LOG = logging.getLogger(__name__)

class PhEDExSiteInfoSource(SiteInfoSource):
    """SiteInfoSource for PhEDEx. Also use CMS Site Status Board for additional information."""

    def __init__(self, config = None):
        config = Configuration(config)

        SiteInfoSource.__init__(self, config)

        self._phedex = PhEDEx(config.get('phedex', None))
        self._ssb = SiteStatusBoard(config.get('ssb', None))

        self.ssb_cache_lifetime = config.get('ssb_cache_lifetime', 3600)
        self._ssb_cache_timestamp = 0
        self._caching_lock = threading.Lock()

        self._waitroom_sites = set()
        self._morgue_sites = set()

    def get_site(self, name): #override
        if not self.check_allowed_site(name):
            LOG.info('get_site(%s)  %s is excluded by configuration.', name, name)
            return None

        LOG.info('get_site(%s)  Fetching information of %s from PhEDEx', name, name)

        # General site info
        result = self._phedex.make_request('nodes', ['node=' + name])
        if len(result) == 0:
            return None

        entry = result[0]

        host = entry['se']
        storage_type = Site.storage_type_val(entry['kind'])

        return Site(name, host = host, storage_type = storage_type)

    def get_site_list(self): #override
        LOG.info('get_site_list  Fetching the list of nodes from PhEDEx')

        site_list = []

        for entry in self._phedex.make_request('nodes'):
            if not self.check_allowed_site(entry['name']):
                continue

            site_list.append(Site(entry['name'], host = entry['se'], storage_type = Site.storage_type_val(entry['kind'])))

        return site_list

    def get_site_status(self, site_name): #override
        with self._caching_lock:
            if time.time() > self._ssb_cache_timestamp + self.ssb_cache_lifetime:
                self._waitroom_sites = set()
                self._morgue_sites = set()
    
                latest_status = {}
    
                # get list of sites in waiting room (153) and morgue (199)
                for colid, stat, sitelist in [(153, Site.STAT_WAITROOM, self._waitroom_sites), (199, Site.STAT_MORGUE, self._morgue_sites)]:
                    result = self._ssb.make_request('getplotdata', 'columnid=%d&time=2184&dateFrom=&dateTo=&sites=all&clouds=undefined&batch=1' % colid)
                    for entry in result:
                        site = entry['VOName']
                        
                        # entry['Time'] is UTC but we are only interested in relative times here
                        timestamp = time.mktime(time.strptime(entry['Time'], '%Y-%m-%dT%H:%M:%S'))
                        if site in latest_status and latest_status[site][0] > timestamp:
                            continue
        
                        if entry['Status'] == 'in':
                            latest_status[site] = (timestamp, stat)
                        else:
                            latest_status[site] = (timestamp, Site.STAT_READY)
    
                for site, (_, stat) in latest_status.items():
                    if stat == Site.STAT_WAITROOM:
                        self._waitroom_sites.add(site)
                    elif stat == Site.STAT_MORGUE:
                        self._morgue_sites.add(site)
    
                self._ssb_cache_timestamp = time.time()

        if site_name in self._waitroom_sites:
            return Site.STAT_WAITROOM
        elif site_name in self._morgue_sites:
            return Site.STAT_MORGUE
        else:
            return Site.STAT_READY

    def get_filename_mapping(self, site_name): #override
        tfc = self._phedex.make_request('tfc', ['node=' + site_name])['array']

        conversions = {}
        for elem in tfc:
            if elem['element_name'] != 'lfn-to-pfn':
                continue

            if 'destination-match' in elem and re.match(elem['destination-match'], site_name) is None:
                continue

            if 'chain' in elem:
                chain = elem['chain']
            else:
                chain = None

            result = elem['result']
            i = 1
            while '$' in result:
                result = result.replace('$%d' % i, '{%d}' % (i - 1))
                i += 1
                if i == 100:
                    # can't be possibly right
                    break

            result = result.replace('\\', '')

            if elem['protocol'] in conversions:
                conversions[elem['protocol']].append((elem['path-match'], result, chain))
            else:
                conversions[elem['protocol']] = [(elem['path-match'], result, chain)]

        def make_mapping_chains(rule):
            if rule[2] is None:
                return [[(rule[0], rule[1])]]
            else:
                if rule[2] not in conversions:
                    return None

                chains = []
                for chained_rule in conversions[rule[2]]:
                    mapped_chains = make_mapping_chains(chained_rule)
                    if mapped_chains is None:
                        continue

                    chains.extend(mapped_chains)

                for chain in chains:
                    chain.append((rule[0], rule[1]))

                return chains

        mappings = {}

        for protocol, rules in conversions.items():
            if protocol == 'direct':
                continue

            if protocol == 'srmv2':
                # for historic reasons PhEDEx calls gfal2 srmv2
                protocol = 'gfal2'

            mapping = []
            
            for rule in rules:
                chains = make_mapping_chains(rule)
                if chains is None:
                    continue

                mapping.extend(chains)

            mappings[protocol] = mapping

        return mappings
