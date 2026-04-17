# Cheeksbase Examples

Example configurations and usage patterns for Cheeksbase.

## Connector Examples

- [Stripe](stripe-example.yaml) - Payments data connector
- [CSV Files](csv-example.yaml) - Local CSV file connector
- [PostgreSQL](postgres-example.yaml) - Database connector

## Usage Examples

### Adding a connector

```bash
cheeksbase init
cheeksbase add stripe --api-key sk_test_...
cheeksbase sync stripe
cheeksbase query "SELECT * FROM stripe.customers LIMIT 10"
```

### Working with mutations

```bash
cheeksbase query "UPDATE stripe.customers SET email = 'new@example.com' WHERE id = 'cus_123'"
cheeksbase mutations --status pending
cheeksbase confirm mut_abc123
```

### Semantic annotations

After syncing, Cheeksbase automatically annotates tables with descriptions,
PII flags, and detected relationships.

```bash
cheeksbase describe stripe.customers --pretty
```
