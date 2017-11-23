# edit and rename to credentials.py
# AZURE CREDENTIALS

AZ_TENANT_ID = ""
"""AZURE Tenant ID"""
AZ_SUBSCRIPTION_ID = ""
"""AZURE Subscription ID"""
AZ_APPLICATION_ID = ""
"""AZURE Application ID"""
AZ_SECRET = ""
"""AZURE Application Secret"""

# AWS CREDENTIALS

AWS_ACCESS_ID = ""
"""AWS Access ID"""
AWS_SECRET_KEY = ""
"""AWS Secret"""

# ssh-keygen -t rsa -b 2048
AZ_KEY_NAME = "az_id_rsa"
"""Name of the RSA 2048 key"""
AZ_PUB_KEY_PATH = '/home/ubuntu/.ssh/' + AZ_KEY_NAME + '.pub'
"""AZURE Public Key Path (RSA 2048)"""
AZ_PRV_KEY_PATH = '/home/ubuntu/.ssh/' + AZ_KEY_NAME
"""AZURE Private Key Path (RSA 2048)"""
