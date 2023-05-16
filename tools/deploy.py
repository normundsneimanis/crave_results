"""
Creates packages for deployment
"""
import argparse
import shutil
import os

argument_parser = argparse.ArgumentParser()
argument_parser.add_argument("--target", type=str, choices=['server', 'client'])
args = argument_parser.parse_args()

if os.path.isdir('release'):
    shutil.rmtree('release')

os.mkdir('release')

common_files = ['CraveBase.py', 'CraveResults.py', 'CraveResultsCommand.py', 'CraveResultsHyperopt.py',
                'CraveResultsLogType.py', 'GzipRotator.py', 'SharedStatus.py', 'tools/generate_keypair.py']

if args.target == 'server':
    path = os.path.join('release', 'crave-results-server')
    os.mkdir(path)
    for f in ['tools/startup-scripts/crave-results-server.service', '.env.example', 'install_server.md', 'SqlDb.py',
              'crave_results_server.py'] + common_files:
        shutil.copyfile(f, os.path.join(path, os.path.basename(f)))

    shutil.make_archive("crave-results-server", "gztar", 'release', 'crave-results-server')

elif args.target == 'client':
    path = os.path.join('release', 'crave-results-client')
    os.mkdir(path)
    for f in ['readme.md'] + common_files:
        shutil.copyfile(f, os.path.join(path, os.path.basename(f)))

    shutil.make_archive("crave-results-client", "gztar", 'release', 'crave-results-client')

