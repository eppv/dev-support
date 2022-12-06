
import subprocess

venv_dir = r''
dbt_dir = r''


def dbt_run_local(config):

    activate_venv = rf'{venv_dir}\Scripts\activate.bat'
    change_to_dbt_dir = f'cd {dbt_dir}'
    commands = [activate_venv, change_to_dbt_dir]

    dbt_cmds = config['dbt']['run']
    for cmd in dbt_cmds:
        commands.append(cmd)

    cmd_str = '; '.join(commands)

    output = subprocess.run(cmd_str, shell=True, capture_output=True)
    print(output.stdout.decode())
