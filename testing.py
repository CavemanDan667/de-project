import subprocess

user = subprocess.check_output('whoami')

print(user==b'runner\n')

print(user)