"""
Name: AIPSnapshotCleaner.py

Author: Guru Pai

Date: Thu 05/24/2019 

# TODO
This python script inserts two background facts into AAD.
1. 
2. 

Arguments:
1. 

NOTE:
By design, this script populates the background fact for a single app.

Prerequisites:
1. Before running this script, ensure that you have these two background facts in the application's assessment
   model and have generated a snapshot using that assessment model.
2. The app and the new snapshot must be conslidated in the HD that is being used. Be sure to update the HD URL below.
"""
__version__ = 1.0

import os
import json
import logging
import sys
import time
import urllib.request
import requests
import yaml
import traceback

from xml.dom import minidom
from datetime import date
from datetime import datetime
from operator import itemgetter
from subprocess import PIPE, STDOUT, DEVNULL, run, CalledProcessError

import DMTInfo as DMT
import VerInfo as Ver

# Logger settings.
logger = logging.getLogger(__name__)
shandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(filename)s [%(funcName)30s:%(lineno)-4d] %(levelname)-8s - %(message)s')
shandler.setFormatter(formatter)
logger.addHandler(shandler)
logger.setLevel(logging.DEBUG)

# Global vars
base_url = ''
domain = ''
username = ''
password = ''
CAST_HOME = ''

apps = []
connection_profiles = []
snapshot_info = []

config_settings = {}

delete_snapshots = False
app_name = ''

def read_yaml():
    global config_settings

    try:
        with open('resources\\AIPCleaner.yaml') as y_file:
            config_settings = yaml.safe_load(y_file)
    except (FileNotFoundError, IOError) as exc:
        logger.error('An IO exception occurred while opening YAML file. Error: %s' % (exc))
        raise Exception('An IO exception occurred while opening YAML file. Error: ' + (exc))
    except yaml.YAMLError as exc:
        logger.error('An exception occurred while reading YAML file. Error: %s' % (exc))
        raise Exception('An exception occurred while reading YAML file. Error: ' + (exc))
    except:
        logger.error('An unknown exception occurred while reading YAML file.')
        raise Exception('An unknown exception occurred while reading YAML file.')
    finally:
        logger.info('Setting successfully retireved from the YAML file.')

def read_pmx(connection_profiles):
    pmx_file = config_settings['CMS']['pmx_file']

    logger.debug('PMX File:%s' % pmx_file)

    try:
        with minidom.parse(pmx_file) as dom:
            cps = dom.getElementsByTagName('connectionprofiles.ConnectionProfilePostgres')

            for cp in cps:
                name = cp.getAttribute('name')
                schema = cp.getAttribute('schema')
                connection_profiles.append({"name": name, "schema": schema})
    finally:
            logger.debug('Names found: %s' % connection_profiles)

def get_apps(apps):
    data = []
    response = ''
    id = ''
    name = ''
    db = ''
    mngt_schema = ''

    # Retrieve names of all apps.
    __headers = {'Accept':'application/json'}

    url =  base_url + '/' + domain + '/applications/'
    auth = (username, password)

    logger.debug('url:%s' % url)

    try:
        with requests.get(url, headers=__headers, auth=auth, stream=True) as response:
            response.raise_for_status()

            if (response.status_code == requests.codes.ok):
                data = response.json()

            for item in data:
                name = item['name']
                if (len(app_name) > 0 and app_name.lower() != name.lower()):
                    continue
                id = item['href'].split('/')[-1]
                db = item['adgDatabase']
                mngt_schema = db.replace('_central', '_mngt')

                apps.append({'id': id, 'name': name, 'adgDatabase': db, 'mngt_schema': mngt_schema})

                logger.debug('Found apps:id:%s; name:%s; adgDatabase:%s; mngt_schema:%s' % (id, name, db, mngt_schema))

            return True
    except (requests.HTTPError) as exc:
        logger.error('requests.get failed while retrieving list of applications. Message:%s' % (str(exc)))
        raise
    #except requests.RequestException as exc:
        #logger.error('requests.get failed, while retrieving list of applications. Message:%s' % (str(exc)))
        #raise
    #except requests.TimeOut as exc:
        #logger.error('requests.get timedout while retrieving list of applications. Message:%s' % (str(exc)))
        #raise

def get_dmt_info(dmt_info_list):
    """
    """
    dmt_app_name = ''
    app_uuid = ''
    delivery_index_file = ''

    app_ver_list = []

    delivery_folder = config_settings['CMS']['delivery_folder']

    # Spin thru the apps and retrieve application info.

    delivery_index_file = delivery_folder + '\data\index.xml'
    logger.debug('Delivery index File:%s' % delivery_index_file)

    """
    TODO:
    This may be a problem, when simply removing version name and deleting the delivery....
    Two ghost entries for a deleted version - V1
    <entry key="43da62fe-173f-43d1-a9f5-599ace271d60_syncId">14</entry>
    <entry key="43da62fe-173f-43d1-a9f5-599ace271d60_uuid">43da62fe-173f-43d1-a9f5-599ace271d60</entry>
    """
    try:
        with minidom.parse(delivery_index_file) as dom:

            entries = dom.getElementsByTagName('entry')

            for entry in entries:
                key = entry.getAttribute('key')

                if len(entry.childNodes):
                    data = entry.childNodes[0].nodeValue
                else:
                    data = 'No Value'

                #logger.debug('key:%s' % key)
                #logger.debug('data:%s' % data)

                # Get UUID and Name.

                if ('_uuid' in key):
                    # UUID is usually the last entry for an app in the index file.
                    # So, once we have the app's UUID, we can save info and look for the next app.

                    app_uuid = data
                    logger.debug('uuid:%s' % app_uuid)

                    # When a new application is registerd in AICP, you will 
                    # find an entry for it in index.xml file and an entity file, but nothing else.
                    # So, the following call may not return any values.

                    get_app_versions(delivery_folder, app_uuid, app_ver_list)

                    if (dmt_app_name != '' and app_uuid != ''):
                        if (dmt_app_name.lower() == app_name.lower()):
                            dmt = DMT.DMTInfo(dmt_app_name, app_uuid, app_ver_list)
                            logger.info('App:%s; Name:%s; Number of versions:%d' % (app_uuid, dmt_app_name, len(app_ver_list)))
                            dmt_info_list.append(dmt)

                        app_uuid = ''
                        dmt_app_name = ''
                        app_ver_list = []
                elif ('_name' in key):
                    # Got name
                    dmt_app_name = data
                    logger.debug('name:%s' % dmt_app_name)

    except (TypeError, AttributeError) as dom_exc:
        logger.error('An exception occurred while reading delivery index file. Cannot continue..')
        raise

def get_app_versions(delivery_folder, app_uuid, app_ver_list):
    """
    """

    ver_uuid = ''
    ver_name = ''
    ver_status = ''
    ver_date = ''
    ver_entity_file = ''
    ver_has_prev_ver = False
    ver_index_file  = ''

    # Spin thru the apps and retrieve version information.
    # If the file does not exist, skip and move to the next app.

    ver_index_file = delivery_folder + '\data\{' + app_uuid + '}\index.xml'
    logger.debug('Delivery index File:%s' % ver_index_file)

    if not os.path.exists(ver_index_file):
        logger.warn('This DMT version file does not exist. Please check. Skipping:%s' % ver_index_file)
        return False

    # Read and save all the versions for the given application.

    try:
        with minidom.parse(ver_index_file) as dom:

            entries = dom.getElementsByTagName('entry')

            for entry in entries:
                key = entry.getAttribute('key')

                if len(entry.childNodes):
                    data = entry.childNodes[0].nodeValue
                else:
                    data = 'No Value'

                #logger.debug('key:%s' % key)
                #logger.debug('data:%s' % data)

                # Get UUID, date and name from the index file.
                # Grab the previous version from the entity file and set 'has_prev_ver'.

                # UUID is the last entry key for each app.
                # Once we hit that, we can save the values and move on to the next apps.

                if ('_uuid' in key):
                    # Got name
                    ver_uuid = data
                    logger.debug('Version uuid:%s' % ver_uuid)

                    # Get pervious version entry from the entity file.

                    ver_entity_file = delivery_folder + '\data\{' + app_uuid + '}\\' + ver_uuid + '.entity.xml'
                    logger.debug('Version entity File:%s' % ver_entity_file)

                    # TODO - Error handling
                    ver_has_prev_ver = get_prev_version(ver_entity_file)

                    app_ver_list.append(Ver.VerInfo(ver_uuid, ver_name, ver_status, ver_date, ver_entity_file, ver_has_prev_ver))

                    logger.info('Ver Info - UUID:%s; Name:%s; Status:%s; Date:%s; Entity file:%s, Has prev ver:%r' %
                        (ver_uuid, ver_name, ver_status, ver_date, ver_entity_file, bool(ver_has_prev_ver)))

                    ver_uuid = ''
                    ver_name = ''
                    ver_status = ''
                    ver_date = ''
                    ver_entity_file = ''
                    ver_has_prev_ver = False
                elif ('_date' in key):
                    ver_date = data
                elif ('_name' in key):
                    ver_name = data
                elif ('_serverStatus' in key):
                    ver_status = data

    except (TypeError, AttributeError) as dom_exc:
        logger.error('An exception occurred while reading delivery index file. Cannot continue..')
        raise
    
    return True

def get_prev_version(ver_entity_file):
    has_prev_ver = False;

    # If the entity file does not exist, skip and move to the next app.
    if not os.path.exists(ver_entity_file):
        logger.error('This DMT entity file does not exist. Please check. Skipping:%s' % ver_entity_file)
        return False

    # Look for the previousVersionEntry attribute and if found, set the flag to true.
    # TODO: Error handling

    try:
        with minidom.parse(ver_entity_file) as dom:

            prev_ver = ''
            versions = dom.getElementsByTagName('delivery.Version')

            # Though expecting only one version, looping, just in case.

            for ver in versions:
                prev_ver = ver.getAttribute('previousVersionEntry')

                if (prev_ver != ''):
                    has_prev_ver = True

                logger.debug('Previous version exists?:%s' % has_prev_ver)

    except (TypeError, AttributeError) as dom_exc:
        logger.error('An exception occurred while reading delivery index file. Cannot continue..')
        raise

    return has_prev_ver

def cleanup_deliveries(app_name, profile_name, dmt_info, log_folder):
    """
    Deletes the deliveries for the given app.
    """

    cli_command = ''

    # Form the CLI command
    skipCnt=0
    original_list=dmt_info.get_versions()
    sorted_version_list = sorted(original_list, key=lambda x: x.date, reverse=True)
    for version in sorted_version_list: 
        # Before initiating the cleanup, update the previousVersionEntry attribute
        # in the entity file, so that the DeleteVersion command works.
        # Otherwise, CMS-CLI will not let us drop the dependent version.
        try:
            logger.info('Clearing the previous version for version:%s', version.get_name())
            version.clear_prev_version()
        except:
            raise

    for version in sorted_version_list: 
        status = version.get_status()
        if status == 'delivery.StatusReadyForAnalysisAndDeployed':
            if skipCnt < 5:
                skipCnt+=1
                continue
#        else: 
#            continue

        cli_command = '"' + CAST_HOME
        cli_command += '\\cast-ms-cli.exe" DeleteVersion -connectionProfile "'
        cli_command += profile_name
        cli_command += '" -appli '
        cli_command += app_name 
        cli_command += ' -version "'
        cli_command += version.get_name() 
        cli_command += '" -logRootPath "'
        # TODO: This should go to CAST_LOG folder.
        cli_command += log_folder
        cli_command += '"'

        logger.debug('CLI Command:%s' % cli_command)

        exec_cli(cli_command)


def exec_cli(cli):
    cli_str = ''.join(cli)
    try:
        logger.info('Calling CLI:%s' % cli_str)

        cli_cmd=run(cli_str, stdout=PIPE, stderr=STDOUT, shell=True, check=True)

        logger.debug('returncode:%s' % cli_cmd.returncode)
        logger.debug('stdout:%s' % cli_cmd.stdout)
        logger.debug('stderr:%s' % cli_cmd.stderr)

        cli_cmd.check_returncode()
    except CalledProcessError as exc:
        logger.error('An error occurred while executing CLI:%d. CLI:%s' % (exc.returncode, exc.cmd))
        

def main():
    global base_url, domain, username, password, CAST_HOME

    profile_name = ''
    apps = []
    connection_profiles = []
    dmt_info_list = []

    try:
        # Read the YAML file to get the config settings.
        read_yaml()

        # Set some global vars
        base_url = config_settings['Dashboard']['URL']
        domain = config_settings['Dashboard']['domain']
        username = config_settings['Dashboard']['username']
        password = config_settings['Dashboard']['password']
        CAST_HOME = config_settings['other_settings']['cast_home']

        # Setup logging to file
        log_folder = config_settings['other_settings']['log_folder']
        log_file = log_folder + '\\AIP_DMTCleaner' + time.strftime('%Y%m%d%H%M%S') + '.log'
        fhandler = logging.FileHandler(log_file, 'w')
        fhandler.setFormatter(formatter)
        logger.addHandler(fhandler)

        # Read the CAST-MS conection profile file to retrieve profile names.
        read_pmx(connection_profiles)
        # Grab names of all apps from the dashboard via REST call.
        get_apps(apps)
        # Retireve a list of snapshots from the dashboard via REST call.
        get_dmt_info(dmt_info_list)

        for app in apps:
            app_name = app['name']
            logger.info('Processing application:%s' % app_name)

            # Get the DMT info for this app.
            # TODO: Find a better way to look this up.

            entry_found = False

            for dmt_info in dmt_info_list:
                if dmt_info.get_app_name() == app_name:
                    entry_found = True
                    break

            if not (entry_found):
                logger.warn('DMT entry NOT found for application:%s.. Skipping' % app_name)
            else:
                logger.info('DMT entry found for application:%s' % app_name)

                # Find the CMS profile name and pass it on the the function.
                profile_name = ''

                for profile in connection_profiles:
                    if profile['schema'] == app['mngt_schema']:
                        profile_name = profile['name']
                        break

                if (profile_name != ''):
                    if (not delete_snapshots):
                        logger.info('For information only: application %s versions would be removed if -drop flag were set' % app['name'])
                    else:
                        cleanup_deliveries(app_name, profile_name, dmt_info, log_folder)
                else:
                    logger.warn('A CMS profile entry was not found for app:%s.. Skipping' % app['name'])

    except BaseException as ex:
        logger.error('Aborting due to a prior exception. %s' % (str(ex)) )
        sys.exit(6)

# Start here

if __name__ == "__main__":
    logger.info('Starting process')
    args = sys.argv[1:]

    count = len(args)

    # TODO: Should be able to clanup a single app.

    if (count > 0):
        for index, arg in enumerate(args):
            if (arg == '-drop'):
                logger.info('The -drop argurment activated. Snapshots will be dropped.')
                delete_snapshots = True
            elif (arg == '-app'):
                if (count < index+1):
                    logger.error('No application for -app argument')
                    sys.exit(1)
                else:
                    index += 1
                    app_name=args[index]
                    logger.info('-app flag found, only deliveries for ' + app_name + ' will be deleted.')

    elif (count > 3):
        logger.error('Invalid number of arguments passed, when expecting one. Aborting.')
        sys.exit(1)
    else:
        # Zero args passed. Which means that the deliveries should not be deleted.
        # Deliveries marked for deletion will only be listed for INFO ONLY.
        logger.info('The -drop argurment was not passed in, no deliveies will be removed.')
        logger.info('To remove deliveries invoke delivery clearner with a -drop argument.')

    main()

