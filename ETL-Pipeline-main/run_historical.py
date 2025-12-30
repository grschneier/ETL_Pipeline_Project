import argparse
import json
import os
import subprocess


def load_clients(mapping_file: str):
    with open(mapping_file, 'r') as f:
        data = json.load(f)
    return list(data.keys())


def run_for_client(client: str, start: str, end: str, output: str):
    cmd = [
        'python',
        'util/historical_fetch.py',
        client,
        '--start', start,
        '--end', end,
        '--output', output
    ]
    env = os.environ.copy()
    env['PYTHONPATH'] = '.'
    subprocess.run(cmd, check=True, env=env)


def main():
    parser = argparse.ArgumentParser(
        description='Run historical_fetch.py for all clients in map.json'
    )
    parser.add_argument('--start', required=True, help='Start date YYYY-MM-DD')
    parser.add_argument('--end', required=True, help='End date YYYY-MM-DD')
    parser.add_argument(
        '--output', choices=['sql', 'csv'], default='csv',
        help='Output format for historical_fetch.py'
    )
    parser.add_argument(
        '--map-file', default='map.json',
        help='Path to mapping json file'
    )
    args = parser.parse_args()

    clients = load_clients(args.map_file)
    for client in clients:
        print(f'Running historical fetch for {client}')
        run_for_client(client, args.start, args.end, args.output)


if __name__ == '__main__':
    main()
