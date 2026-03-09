"""District9 D9Portal token launcher — Mode B (0x9999 suffix)."""

import json
import os
import re
import time

import requests
from eth_account import Account
from web3 import Web3

from ..config import OpenClawConfig
from ..utils.logger import log
from .constants import (
    AGENT_SHARE_BPS,
    D9_BASE_URL,
    D9_PORTAL_CONTRACTS,
    D9_TOKEN_SUFFIX,
    DISTRICT9_SHARE_BPS,
    DISTRICT9_TREASURY,
    FLAP_UPLOAD_API,
)

# ── Retry / validation constants ──────────────────────────────
IPFS_MAX_RETRIES = 3
IPFS_RETRY_DELAY = 2  # seconds
TX_RECEIPT_TIMEOUT = 120  # seconds

# Minimal ERC20 ABI for approve + balanceOf
ERC20_ABI = [
    {"inputs": [{"name": "account", "type": "address"}], "name": "balanceOf",
     "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "name": "approve", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "nonpayable", "type": "function"},
]

# D9Portal ABI — createToken + buy + sell + TokenCreated event
D9_PORTAL_ABI = [
    {
        "inputs": [
            {"name": "name", "type": "string"},
            {"name": "symbol", "type": "string"},
            {"name": "meta", "type": "string"},
            {"name": "salt", "type": "bytes32"},
            {
                "components": [
                    {"name": "recipient", "type": "address"},
                    {"name": "bps", "type": "uint16"},
                ],
                "name": "vaultRecipients",
                "type": "tuple[]",
            },
        ],
        "name": "createToken",
        "outputs": [{"name": "token", "type": "address"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "token", "type": "address"},
            {"name": "minTokensOut", "type": "uint256"},
        ],
        "name": "buy",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "token", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "minBnbOut", "type": "uint256"},
        ],
        "name": "sell",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "token", "type": "address"},
            {"indexed": True, "name": "vault", "type": "address"},
            {"indexed": True, "name": "creator", "type": "address"},
        ],
        "name": "TokenCreated",
        "type": "event",
    },
]


class District9Launcher:
    """Launch tokens through D9Portal with 0x9999 vanity suffix."""

    def __init__(self, config: OpenClawConfig):
        self.config = config
        self.account = Account.from_key(config.wallet.private_key)

        chain_key = config.chain
        self.chain = D9_PORTAL_CONTRACTS.get(chain_key)
        if not self.chain:
            raise ValueError(f"District9 Portal not available on: {chain_key}")

        self.w3 = Web3(Web3.HTTPProvider(self.chain["rpc"]))
        if not self.w3.is_connected():
            raise ConnectionError(f"Cannot connect to {self.chain['rpc']}")

    # ── Validation helpers ─────────────────────────────────────

    @staticmethod
    def _validate_metadata(metadata: dict):
        """Validate token metadata before launch."""
        name = metadata.get("name", "")
        symbol = metadata.get("symbol", "")
        if not name or len(name) > 32:
            raise ValueError(f"Invalid token name: must be 1-32 chars, got '{name}'")
        if not symbol or len(symbol) > 10 or not symbol.isalnum():
            raise ValueError(f"Invalid symbol: must be 1-10 alphanumeric chars, got '{symbol}'")

    def _check_gas_price(self):
        """Abort if gas price exceeds configured maximum."""
        max_gwei = self.config.launch.max_gas_gwei
        if max_gwei <= 0:
            return
        current = self.w3.eth.gas_price
        current_gwei = current / 1e9
        if current_gwei > max_gwei:
            raise RuntimeError(
                f"Gas price too high: {current_gwei:.1f} gwei > {max_gwei} gwei limit. "
                f"TX aborted to protect funds."
            )

    def launch(self, metadata: dict, image_path: str = "") -> dict:
        """
        Launch a token through D9Portal.

        Flow: Validate → Find salt (9999) → Upload IPFS → createToken → initial buy.
        Same interface as FlapLauncher.launch().
        """
        # Step 0: Validate inputs and gas price
        self._validate_metadata(metadata)
        self._check_gas_price()

        # Step 1: Find CREATE2 salt (9999 suffix, deployer = D9Portal)
        token_impl = self.chain["tax_token_v1_impl"]
        d9_portal = self.chain["d9_portal"]
        salt, predicted_addr = self._find_salt(token_impl, d9_portal)

        # Step 2: Set website URL and upload to IPFS
        if not metadata.get("website"):
            metadata["website"] = f"{D9_BASE_URL}/token/{predicted_addr}"
        cid = self._upload_to_ipfs(metadata, image_path)
        image_cid = self._resolve_image_cid(cid)

        # Step 3: Create token
        result = self._send_create_tx(metadata, cid, salt)

        # Step 4: Initial buy (if configured)
        token_addr = result["contract_address"]
        quote_amt = self.w3.to_wei(float(self.config.launch.initial_buy), "ether")
        if quote_amt > 0:
            self._send_buy_tx(token_addr, quote_amt)

        # Step 5: Auto-sell (if configured)
        if self.config.launch.auto_sell and quote_amt > 0:
            pct = self.config.launch.sell_percentage
            self._send_sell_tx(token_addr, percentage=pct, buy_cost_wei=quote_amt)

        # Add convenience URLs
        explorer = self.chain["explorer"]
        result.update({
            "predicted_address": predicted_addr,
            "ipfs_cid": cid,
            "image_cid": image_cid,
            "explorer_tx": f"{explorer}/tx/{result['tx_hash']}",
            "explorer_token": f"{explorer}/token/{token_addr}",
            "d9_token_url": f"{D9_BASE_URL}/token/{token_addr}",
            "d9_agent_url": f"{D9_BASE_URL}/agent/{self.account.address}",
        })

        # Step 6: Submit metadata to DISTRICT9 website
        self._submit_metadata(token_addr, metadata, cid, image_cid, result["tx_hash"])

        return result

    @staticmethod
    def _resolve_image_cid(metadata_cid: str) -> str:
        """Fetch IPFS metadata JSON and extract the actual image CID."""
        try:
            resp = requests.get(
                f"https://flap.mypinata.cloud/ipfs/{metadata_cid}", timeout=10
            )
            if resp.status_code == 200:
                image = resp.json().get("image", "")
                if image:
                    log.info(f"Image CID resolved: {image}")
                    return image
        except Exception:
            pass
        return metadata_cid  # fallback to metadata CID

    def _submit_metadata(self, token_addr: str, metadata: dict, cid: str, image_cid: str, tx_hash: str):
        """Submit token metadata to DISTRICT9 website for DB indexing."""
        agent_tag = ""
        match = re.search(r"\[D9:([^\]]+)\]", metadata.get("description", ""))
        if match:
            agent_tag = f"[D9:{match.group(1)}]"

        payload = {
            "json": {
                "address": token_addr,
                "name": metadata["name"],
                "symbol": metadata["symbol"],
                "ipfsCid": cid,
                "logoUrl": f"https://flap.mypinata.cloud/ipfs/{image_cid}",
                "description": metadata.get("description", ""),
                "creator": self.account.address,
                "agentTag": agent_tag,
                "txHash": tx_hash,
                "mode": "b",
            }
        }

        try:
            resp = requests.post(
                f"{D9_BASE_URL}/api/trpc/tokens.submit",
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                log.info("Metadata submitted to DISTRICT9 website")
            else:
                log.warning(f"Metadata submit failed: {resp.status_code} {resp.text}")
        except Exception as e:
            log.warning(f"Metadata submit failed: {e}")

    def _upload_to_ipfs(self, metadata: dict, image_path: str = "") -> str:
        """Upload token metadata to Flap's IPFS via GraphQL mutation."""
        mutation = """
        mutation Create($file: Upload!, $meta: MetadataInput!) {
          create(file: $file, meta: $meta)
        }
        """

        meta = {
            "website": metadata.get("website", ""),
            "twitter": metadata.get("twitter") or None,
            "telegram": metadata.get("telegram") or None,
            "description": metadata.get("description", ""),
            "creator": "0x0000000000000000000000000000000000000000",
        }

        operations = json.dumps({
            "query": mutation,
            "variables": {"file": None, "meta": meta},
        })
        mapping = json.dumps({"0": ["variables.file"]})

        if image_path and os.path.exists(image_path):
            with open(image_path, "rb") as f:
                image_data = f.read()
            filename = os.path.basename(image_path)
        else:
            import base64
            png_b64 = (
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4"
                "2mP8/58BAwAI/AL+hc2rNAAAAABJRU5ErkJggg=="
            )
            image_data = base64.b64decode(png_b64)
            filename = "logo.png"

        files = {
            "operations": (None, operations, "application/json"),
            "map": (None, mapping, "application/json"),
            "0": (filename, image_data, "image/png"),
        }

        log.info("Uploading metadata to IPFS...")
        last_error = None
        for attempt in range(1, IPFS_MAX_RETRIES + 1):
            try:
                resp = requests.post(FLAP_UPLOAD_API, files=files, timeout=30)
                if resp.status_code != 200:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
                data = resp.json()
                if "errors" in data:
                    raise RuntimeError(f"GraphQL errors: {data['errors']}")
                cid = data["data"]["create"]
                log.info(f"Metadata uploaded: {cid}")
                return cid
            except Exception as e:
                last_error = e
                if attempt < IPFS_MAX_RETRIES:
                    log.warning(f"IPFS upload attempt {attempt}/{IPFS_MAX_RETRIES} failed: {e}")
                    time.sleep(IPFS_RETRY_DELAY * attempt)
        raise RuntimeError(f"IPFS upload failed after {IPFS_MAX_RETRIES} attempts: {last_error}")

    def _find_salt(self, token_impl: str, deployer: str) -> tuple[bytes, str]:
        """Find CREATE2 salt for vanity address (9999 suffix)."""
        impl_hex = token_impl[2:].lower()

        # EIP-1167 minimal proxy bytecode
        bytecode_hex = (
            "3d602d80600a3d3981f3"
            "363d3d373d3d3d363d73"
            + impl_hex
            + "5af43d82803e903d91602b57fd5bf3"
        )
        bytecode = bytes.fromhex(bytecode_hex)
        bytecode_hash = Web3.keccak(bytecode)
        deployer_bytes = bytes.fromhex(deployer[2:].lower())

        log.info("Finding CREATE2 salt (9999 suffix)...")
        seed = Account.create().key
        salt = Web3.keccak(seed)
        iterations = 0
        start = time.time()

        while True:
            data = b"\xff" + deployer_bytes + salt + bytecode_hash
            addr_hash = Web3.keccak(data)
            addr = Web3.to_checksum_address(addr_hash[-20:].hex())

            if addr.lower().endswith(D9_TOKEN_SUFFIX):
                elapsed = time.time() - start
                log.info(f"Salt found in {iterations} iterations ({elapsed:.1f}s): {addr}")
                return salt, addr

            salt = Web3.keccak(salt)
            iterations += 1

    def _send_create_tx(self, metadata: dict, cid: str, salt: bytes) -> dict:
        """Build, sign, and send the D9Portal.createToken transaction."""
        self._check_gas_price()
        d9_portal_addr = self.chain["d9_portal"]
        portal = self.w3.eth.contract(
            address=Web3.to_checksum_address(d9_portal_addr),
            abi=D9_PORTAL_ABI,
        )

        # SplitVault recipients: 50% DISTRICT9 + 50% Agent
        recipients = [
            (Web3.to_checksum_address(DISTRICT9_TREASURY), DISTRICT9_SHARE_BPS),
            (Web3.to_checksum_address(self.account.address), AGENT_SHARE_BPS),
        ]

        wallet = self.account.address
        nonce = self.w3.eth.get_transaction_count(wallet)
        balance = self.w3.eth.get_balance(wallet)

        log.info(f"Wallet: {wallet}")
        log.info(f"Balance: {self.w3.from_wei(balance, 'ether')} BNB")
        log.info(f"Split Vault: {DISTRICT9_SHARE_BPS}bps DISTRICT9 + {AGENT_SHARE_BPS}bps Agent")

        # Estimate gas
        try:
            gas_est = portal.functions.createToken(
                metadata["name"], metadata["symbol"], cid, salt, recipients
            ).estimate_gas({"from": wallet})
            gas_limit = int(gas_est * 1.3)
        except Exception as e:
            log.warning(f"Gas estimation failed ({e}), using fallback")
            gas_limit = 3_000_000

        tx = portal.functions.createToken(
            metadata["name"], metadata["symbol"], cid, salt, recipients
        ).build_transaction({
            "from": wallet,
            "value": 0,
            "gas": gas_limit,
            "gasPrice": self.w3.eth.gas_price,
            "nonce": nonce,
            "chainId": self.w3.eth.chain_id,
        })

        log.info("Signing and sending createToken()...")
        signed = self.w3.eth.account.sign_transaction(tx, self.account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info(f"TX sent: {tx_hash.hex()}")

        log.info("Waiting for confirmation...")
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=TX_RECEIPT_TIMEOUT)

        if receipt["status"] != 1:
            raise RuntimeError(f"Transaction reverted! TX: {tx_hash.hex()}")

        log.info(f"Confirmed in block {receipt['blockNumber']} (gas: {receipt['gasUsed']})")

        # Parse TokenCreated event
        token_address = None
        try:
            logs = portal.events.TokenCreated().process_receipt(receipt)
            if logs:
                token_address = logs[0]["args"]["token"]
        except Exception:
            pass

        if not token_address:
            token_address = "unknown"
            log.warning("Could not parse token address from event logs")

        return {
            "contract_address": token_address,
            "tx_hash": tx_hash.hex(),
            "block_number": receipt["blockNumber"],
            "gas_used": receipt["gasUsed"],
        }

    def _send_buy_tx(self, token_addr: str, value: int):
        """Send initial buy transaction on the bonding curve with slippage protection."""
        self._check_gas_price()

        d9_portal_addr = self.chain["d9_portal"]
        portal = self.w3.eth.contract(
            address=Web3.to_checksum_address(d9_portal_addr),
            abi=D9_PORTAL_ABI,
        )

        wallet = self.account.address
        token_addr_cs = Web3.to_checksum_address(token_addr)
        nonce = self.w3.eth.get_transaction_count(wallet)

        # Slippage: minTokensOut = 1 for initial buy (first buyer on new curve,
        # token address not yet public, sandwich risk is near-zero)
        min_tokens_out = 1
        log.info(
            f"Initial buy: {self.w3.from_wei(value, 'ether')} BNB "
            f"(slippage: {self.config.launch.slippage_bps}bps, minTokensOut={min_tokens_out})..."
        )

        try:
            gas_est = portal.functions.buy(
                token_addr_cs, min_tokens_out
            ).estimate_gas({"from": wallet, "value": value})
            gas_limit = int(gas_est * 1.3)
        except Exception as e:
            log.warning(f"Buy gas estimation failed ({e}), using 500K")
            gas_limit = 500_000

        tx = portal.functions.buy(
            token_addr_cs, min_tokens_out
        ).build_transaction({
            "from": wallet,
            "value": value,
            "gas": gas_limit,
            "gasPrice": self.w3.eth.gas_price,
            "nonce": nonce,
            "chainId": self.w3.eth.chain_id,
        })

        signed = self.w3.eth.account.sign_transaction(tx, self.account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info(f"Buy TX sent: {tx_hash.hex()}")

        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=TX_RECEIPT_TIMEOUT)
        if receipt["status"] != 1:
            raise RuntimeError(f"Buy TX reverted! {tx_hash.hex()}")

        log.info(f"Buy confirmed in block {receipt['blockNumber']} (gas: {receipt['gasUsed']})")

    def sell(self, token_addr: str, percentage: int = 100):
        """Public method: sell tokens back to D9Portal bonding curve."""
        if not 1 <= percentage <= 100:
            raise ValueError(f"sell_percentage must be 1-100, got {percentage}")
        self._send_sell_tx(token_addr, percentage=percentage)

    def _send_sell_tx(self, token_addr: str, percentage: int = 100, buy_cost_wei: int = 0):
        """Approve tokens and sell back to D9Portal bonding curve with slippage protection.

        Args:
            buy_cost_wei: If known (auto-sell flow), used to estimate minBnbOut.
                         If 0, minBnbOut defaults to 1 wei (prevents zero-output exploit).
        """
        self._check_gas_price()

        wallet = self.account.address
        token_addr = Web3.to_checksum_address(token_addr)
        d9_portal_addr = Web3.to_checksum_address(self.chain["d9_portal"])

        token = self.w3.eth.contract(address=token_addr, abi=ERC20_ABI)
        balance = token.functions.balanceOf(wallet).call()

        if balance == 0:
            log.info(f"No tokens to sell for {token_addr}")
            return

        sell_amount = balance * percentage // 100
        if sell_amount == 0:
            log.info("Calculated sell amount is 0, skipping")
            return

        # Calculate minBnbOut with slippage protection
        slippage_bps = self.config.launch.slippage_bps
        if buy_cost_wei > 0:
            # Auto-sell after buy: expect ~96% back (1% buy fee + 1% sell fee + ~2% price impact)
            expected_bnb = buy_cost_wei * 96 // 100
            min_bnb_out = expected_bnb * (10000 - slippage_bps) // 10000
        else:
            # Standalone sell: no cost reference, use 1 wei floor (prevents zero output)
            min_bnb_out = 1

        log.info(
            f"Selling {percentage}% ({self.w3.from_wei(sell_amount, 'ether')} tokens) "
            f"of {token_addr} (minBnbOut={self.w3.from_wei(min_bnb_out, 'ether')}, "
            f"slippage={slippage_bps}bps)..."
        )

        # Approve
        nonce = self.w3.eth.get_transaction_count(wallet)
        approve_tx = token.functions.approve(d9_portal_addr, sell_amount).build_transaction({
            "from": wallet, "gas": 100_000,
            "gasPrice": self.w3.eth.gas_price,
            "nonce": nonce, "chainId": self.w3.eth.chain_id,
        })
        signed = self.w3.eth.account.sign_transaction(approve_tx, self.account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=TX_RECEIPT_TIMEOUT)
        log.info("Approve confirmed")

        # Sell with slippage protection
        portal = self.w3.eth.contract(address=d9_portal_addr, abi=D9_PORTAL_ABI)
        nonce = self.w3.eth.get_transaction_count(wallet)

        try:
            gas_est = portal.functions.sell(
                token_addr, sell_amount, min_bnb_out
            ).estimate_gas({"from": wallet})
            gas_limit = int(gas_est * 1.3)
        except Exception as e:
            log.warning(f"Sell gas estimation failed ({e}), using 500K")
            gas_limit = 500_000

        sell_tx = portal.functions.sell(
            token_addr, sell_amount, min_bnb_out
        ).build_transaction({
            "from": wallet, "gas": gas_limit,
            "gasPrice": self.w3.eth.gas_price,
            "nonce": nonce, "chainId": self.w3.eth.chain_id,
        })
        signed = self.w3.eth.account.sign_transaction(sell_tx, self.account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info(f"Sell TX sent: {tx_hash.hex()}")

        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=TX_RECEIPT_TIMEOUT)
        if receipt["status"] != 1:
            raise RuntimeError(f"Sell TX reverted! {tx_hash.hex()}")

        log.info(f"Sell confirmed in block {receipt['blockNumber']} (gas: {receipt['gasUsed']})")
