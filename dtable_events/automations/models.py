import logging
from copy import deepcopy

from sqlalchemy.orm import mapped_column
from sqlalchemy import Integer, String, DateTime, Text

from dtable_events.automations.hasher import AESPasswordHasher
from dtable_events.db import Base
import json

logger = logging.getLogger(__name__)


def _decrypt_detail(detail):
    detail_clone = deepcopy(detail)
    cryptor = AESPasswordHasher()
    try:
        if 'password' in detail_clone.keys():
            password = detail_clone.get('password')
            if password:
                detail_clone.update({'password': cryptor.decode(password)})
        if 'webhook_url' in detail.keys():
            webhook_url = detail.get('webhook_url')
            if webhook_url:
                detail_clone.update({'webhook_url': cryptor.decode(webhook_url)})
        if 'api_key' in detail.keys():
            api_key = detail.get('api_key')
            if api_key:
                detail_clone.update({'api_key': cryptor.decode(api_key)})
        if 'secret_key' in detail.keys():
            secret_key = detail.get('secret_key')
            if secret_key:
                detail_clone.update({'secret_key': cryptor.decode(secret_key)})
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
            'account_name': self.account_name,
            'account_type': self.account_type,
            'detail': _decrypt_detail(detail_dict)
        }
        return res


def get_third_party_account(session, account_id):
    account_query = session.query(BoundThirdPartyAccounts).filter(
        BoundThirdPartyAccounts.id == account_id
    )
    account = account_query.first()
    if account:
        return account.to_dict()
    else:
        logger.warning("Third party account %s does not exists." % account_id)
        return None
