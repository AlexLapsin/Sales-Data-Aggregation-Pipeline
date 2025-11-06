# ADR 002: Use Key-Pair Authentication for Snowflake

## Status

Accepted

## Context

The pipeline requires secure authentication to Snowflake for Terraform provisioning, dbt transformations, and Python connections. We need a method that balances security, automation, and operational simplicity.

### Requirements

1. Secure authentication without storing passwords in code
2. Support for automated workflows (Terraform, Airflow, dbt)
3. Rotation capability without code changes
4. Compliance with security best practices
5. No interactive prompts in CI/CD pipelines
6. Audit trail for authentication events

### Options Considered

**Option 1: Username and Password**

Pros:
- Simple to implement
- Widely understood
- Works with all tools

Cons:
- Password must be stored in environment variables
- Passwords expire and require rotation
- Less secure than key-based methods
- No MFA support in automation
- Password visible in process lists

**Option 2: OAuth Integration**

Pros:
- Industry standard authentication
- Token-based, no passwords
- Supports SSO providers
- Fine-grained permissions

Cons:
- Complex setup (requires OAuth provider)
- Token refresh logic needed
- Not supported by all Snowflake connectors
- Overkill for service account automation

**Option 3: Key-Pair Authentication**

Pros:
- No password storage required
- Private key encrypted with passphrase
- Stronger security than passwords
- Simple rotation (just update public key)
- Supported by Terraform, dbt, Python connectors
- No expiration (unlike passwords)
- Private key never transmitted

Cons:
- Requires key generation step
- Passphrase still needed (but only for key decryption)
- Not all third-party tools support key-pair

**Option 4: External Browser Authentication**

Pros:
- Leverages SSO
- No stored credentials

Cons:
- Requires interactive browser
- Not suitable for automation
- Cannot use in CI/CD pipelines

## Decision

Use RSA key-pair authentication with encrypted private keys.

### Rationale

1. **Enhanced Security:**
   - Private key never transmitted to Snowflake
   - Key encrypted with passphrase at rest
   - Stronger than password-based authentication
   - Recommended by Snowflake for production

2. **Automation-Friendly:**
   ```python
   # Python connector
   conn = snowflake.connector.connect(
       account="YOUR_ACCOUNT",
       user="YOUR_USER",
       private_key=pkb,  # No password needed
       ...
   )
   ```
   No interactive prompts required.

3. **Terraform Support:**
   ```hcl
   provider "snowflake" {
       private_key            = file(var.SNOWFLAKE_PRIVATE_KEY_PATH)
       private_key_passphrase = var.SNOWFLAKE_KEY_PASSPHRASE
   }
   ```
   Native support in Snowflake provider.

4. **Simple Rotation:**
   ```sql
   -- Update public key in Snowflake
   ALTER USER analytics_user SET RSA_PUBLIC_KEY='NEW_PUBLIC_KEY';
   ```
   No code changes required, just update Snowflake.

5. **Passphrase Protection:**
   - Private key encrypted with AES-256
   - Passphrase only used locally for decryption
   - Passphrase in `.env` (gitignored, not committed)

## Consequences

### Positive

- No passwords stored in environment variables
- Stronger authentication for production
- Easy key rotation without code changes
- Supported by all pipeline tools (Terraform, dbt, Python)
- Audit trail in Snowflake login history
- Complies with enterprise security standards

### Negative

- Initial setup more complex than password
- Requires managing private key files
- Passphrase still in environment (but only for decryption)
- Team members need training on key generation

### Mitigation

- Provide clear documentation for key generation
- Use Terraform to automate public key registration
- Store private keys in secure locations (never commit to Git)
- Use AWS Secrets Manager for production environments
- Document passphrase rotation procedures

## Implementation

### 1. Generate Key Pair

```bash
# Generate 2048-bit RSA private key with encryption
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### 2. Register Public Key in Snowflake

```sql
ALTER USER analytics_user SET RSA_PUBLIC_KEY='YOUR_PUBLIC_KEY_HERE';
```

Or via Terraform:
```hcl
resource "snowflake_user" "analytics" {
  name            = "ANALYTICS_USER"
  rsa_public_key  = file("rsa_key.pub")
}
```

### 3. Configure Environment

```.env
SNOWFLAKE_PRIVATE_KEY_PATH=/config/keys/rsa_key.p8
SNOWFLAKE_KEY_PASSPHRASE=YOUR_PASSPHRASE_HERE
```

### 4. Use in Connections

**Terraform:**
```hcl
provider "snowflake" {
  private_key            = file(var.SNOWFLAKE_PRIVATE_KEY_PATH)
  private_key_passphrase = var.SNOWFLAKE_KEY_PASSPHRASE
}
```

**dbt:**
```yaml
profiles:
  sales_pipeline:
    outputs:
      dev:
        type: snowflake
        private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
        private_key_passphrase: "{{ env_var('SNOWFLAKE_KEY_PASSPHRASE') }}"
```

**Python:**
```python
from cryptography.hazmat.primitives import serialization
import snowflake.connector

with open('/config/keys/rsa_key.p8', 'rb') as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=os.getenv('SNOWFLAKE_KEY_PASSPHRASE').encode(),
        backend=default_backend()
    )

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    private_key=pkb
)
```

## Security Considerations

**Private Key Storage:**
- Never commit to Git (add to `.gitignore`)
- Set file permissions: `chmod 600 rsa_key.p8`
- Use AWS Secrets Manager in production
- Rotate keys every 90 days

**Passphrase Management:**
- Use strong passphrase (16+ characters)
- Store in `.env` (gitignored)
- Use AWS Secrets Manager in production
- Never log or print passphrase

**Audit:**
- Monitor Snowflake login history
- Alert on failed authentication attempts
- Review key usage quarterly

## Alternatives for Future

If requirements change:

**AWS IAM Authentication (Future):**
- If migrating fully to AWS
- Eliminates need for keys
- Uses IAM roles

**HashiCorp Vault:**
- For enterprise secret management
- Dynamic credential generation
- Automatic rotation

**Snowflake Private Link:**
- For network-level security
- Eliminates public internet exposure

## References

- Snowflake Key-Pair Authentication: https://docs.snowflake.com/en/user-guide/key-pair-auth
- OpenSSL Key Generation: https://www.openssl.org/docs/
- Security Best Practices: PROJECT_OVERHAUL_PLAN.md Phase 5

## Date

2024-01-15

## Authors

Data Engineering Team
