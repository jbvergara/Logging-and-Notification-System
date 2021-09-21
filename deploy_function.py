import subprocess as sp
import shutil as sh

from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PACKAGE_DIR = SCRIPT_DIR.joinpath('package')
LAMBDA_SCRIPT = SCRIPT_DIR.joinpath('lambda_function.py')
LAMBDA_PACKAGE = SCRIPT_DIR.joinpath('function.zip')


def run(command, *args):
    cmd = ' '.join([
        command,
        ' '.join(args),
    ])
    print(f'Exec -> {cmd}')
    sp.run([cmd], shell=True)


def cleanup():
    print('Cleaning up...')
    if LAMBDA_PACKAGE.exists():
        LAMBDA_PACKAGE.unlink()
    if PACKAGE_DIR.exists():
        sh.rmtree(PACKAGE_DIR, ignore_errors=True)
    if LAMBDA_SCRIPT.exists():
        LAMBDA_SCRIPT.unlink()


def main(function_name, script_name, requirements=None):
    cleanup()

    # Install the requirements
    if requirements:
        run(
            'pip install',
            f'--target {PACKAGE_DIR}',
            f'-r {requirements}',
        )

    # Create the package
    run(
        f'cd {PACKAGE_DIR};zip',
        '-r9',
        f'{LAMBDA_PACKAGE}',
        '.',
    )

    sh.copy2(script_name, f'{LAMBDA_SCRIPT}')
    run(
        f'cd {SCRIPT_DIR};zip',
        f'{LAMBDA_PACKAGE}',
        f'{LAMBDA_SCRIPT.name}',
    )

    # Deploy
    run(
        'aws lambda update-function-code',
        f'--function-name {function_name}',
        f'--zip-file fileb://{LAMBDA_PACKAGE}',
    )

    cleanup()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Deploys python script to a lambda function.'
    )
    parser.add_argument(
        'function_name',
        help='Lambda function name',
    )
    parser.add_argument(
        'script_name',
        help='Python script',
    )
    parser.add_argument(
        '-r', '--requirements',
        help='Python requirements file',
    )
    args = parser.parse_args()
    main(
        args.function_name, args.script_name,
        requirements=args.requirements
    )
