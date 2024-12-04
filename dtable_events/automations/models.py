import logging
from copy import deepcopy

from sqlalchemy.orm import mapped_column
from sqlalchemy import Integer, String, DateTime, Text, select

from dtable_events.automations.hasher import AESPasswordHasher
from dtable_events.db import Base
import json

logger = logging.getLogger(__name__)

ENCRYPT_KEYS = ['password', 'webhook_url', 'api_key', 'secret_key', 'repo_api_token', 'client_secret']

def _encrypt_detail(detail):
    detail_clone = deepcopy(detail)
    cryptor = AESPasswordHasher()
    try:
        encrypted_details = {
            key: cryptor.encode(detail_clone[key])
            for key in ENCRYPT_KEYS if key in detail_clone and detail_clone[key]
        }
        detail_clone.update(encrypted_details)
        return json.dumps(detail_clone)
    except Exception as e:
        logger.error(e)
        return None
    
def _decrypt_detail(detail):
    detail_clone = deepcopy(detail)
    cryptor = AESPasswordHasher()
    try:
        decrypted_details = {
            key: cryptor.decode(detail_clone[key])
            for key in ENCRYPT_KEYS if key in detail_clone and detail_clone[key]
        }
        detail_clone.update(decrypted_details)
        return detail_clone
    except Exception as e:
        logger.error(e)
        return None

class BoundThirdPartyAccounts(Base):
    __tablename__ = 'bound_third_party_accounts'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    dtable_uuid = mapped_column(String(length=255), nullable=False)
    account_name = mapped_column(String(length=255), nullable=False)
    account_type = mapped_column(String(length=255), nullable=False)
    created_at = mapped_column(DateTime, nullable=False)
    detail = mapped_column(Text)

    def to_dict(self):
        detail_dict = json.loads(self.detail)
        res = {
            'id': self.id,
            'dtable_uuid': self.dtable_uuid,
            'account_name': self.account_name,
            'account_type': self.account_type,
            'detail': _decrypt_detail(detail_dict)
        }
        return res


def get_third_party_account(session, account_id):
    stmt = select(BoundThirdPartyAccounts).where(BoundThirdPartyAccounts.id == account_id).limit(1)
    account = session.scalars(stmt).first()
    if account:
        return account.to_dict()
    else:
        logger.warning("Third party account %s does not exists." % account_id)
        return None
    
def update_third_party_account_detail(session, account_id, new_detail):
    stmt = select(BoundThirdPartyAccounts).where(BoundThirdPartyAccounts.id == account_id).limit(1)
    account = session.scalars(stmt).first()
    if account:
        account.detail = _encrypt_detail(new_detail)
        session.commit()
    else:
        logger.warning("Third party account %s does not exists." % account_id)
        return None
