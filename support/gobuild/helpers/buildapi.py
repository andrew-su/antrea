#!/usr/bin/python -u

import urllib
import urllib2
import json

URL_PREFIX = 'http://buildapi.lvn.broadcom.net'


def retry_open(url, num_tries=10):
    for i in range(num_tries):
        try:
            return urllib2.urlopen(url)
        except urllib2.URLError as exc:
                continue
    raise urllib2.URLError("max retries exceeded")


def last_successful_build(target, branch, build_context="ob", buildtype="beta"):
    """Return build object from gobuild database for last successful build
    given product, branch and build system
    """
    branch_query = urllib.urlencode([('branch', branch)])
    url = ('%s/%s/build/?%s&product=%s&buildtype=%s&_order_by=-id'
           '&_limit=1&buildstate=succeeded' %
           (URL_PREFIX, build_context, branch_query, target, buildtype))
    build_resp = retry_open(url)
    res = build_resp.read()
    if build_resp.getcode() == 200:
        try:
            rv = json.loads(res)['_list'][0]
        except Exception as e:
            raise Exception("%r target=%s branch=%s buildtype=%s url=%s"
                            % (e, target, branch, buildtype, url))
        return rv
    else:
        raise Exception(res)


def get_lastgood_commit(target, branch, build_context="ob", buildtype="beta"):
    """Return latest commit for last successful build"""
    data = last_successful_build(target, branch, build_context,
                                 buildtype=buildtype)
    commit_id = str(data['changeset'])
    return commit_id


def update_component_commits(components, requested_buildtype="beta"):
    """Update component dict with changeset from latest official ob
    params:
        components: component dict
        requested_buildtype: this replaces any buildtype of %(buildtype)
    """

    for target in components:
        if not 'change' in components[target]:
            # get buildtype from dict, or default to requested_buildtype
            buildtype = components[target].get(
                "buildtype", requested_buildtype)
            if buildtype == "%(buildtype)":
                buildtype = requested_buildtype

            # get commit id of last good ob for this component
            commitid = get_lastgood_commit(
                    target, components[target]['branch'],
                    buildtype=buildtype)
            components[target]['change'] = commitid
            '''if type(components[target]) == list:
                list_comps = []
                for c in components[target]:
                       buildtype = c['buildtype']
                       if buildtype == "%(buildtype)":
                           buildtype = requested_buildtype

                       # get commit id of last good ob for this component
                       commitid = get_lastgood_commit(
                               target, c['branch'],
                               buildtype=buildtype)
                       c['change'] = commitid
                       list_comps.append(c)
                components[target] = list_comps
            else:
                buildtype = components[target].get('buildtype', 'beta')
                if buildtype == "%(buildtype)":
                    buildtype = requested_buildtype

                # get commit id of last good ob for this component
                commitid = get_lastgood_commit(
                        target, components[target]['branch'],
                        buildtype=buildtype)
                components[target]['change'] = commitid'''
    return components


def check_build_exist(buildnumber, build_context="ob"):
    """ Return build object from gobuild database for last
        successful build given product, branch and build system """
    build_resp = retry_open(
        '%s/%s/build/?id=%s' % (URL_PREFIX, build_context, buildnumber))
    try:
        json.loads(build_resp.read())['_list'][0]
    except:
        return False
    return True

