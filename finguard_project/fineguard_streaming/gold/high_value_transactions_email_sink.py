from pyspark import pipelines as dp
from pyspark.sql import functions as F
import html


def build_alert_email(row):
    """Build subject and HTML body for a single high value transaction alert."""
    def esc(value):
        return html.escape("" if value is None else str(value))

    subject = f"FinGuard Alert: High value transaction {row['transaction_id']}"
    body = f"""
<html>
<body>
<h2>High Value Transaction Alert</h2>
<p>Dear {esc(row['customer_name'])},</p>
<p>A transaction on your account exceeded the transaction limit you configured.
If you did not perform this transaction, please contact us immediately.</p>
<table border="1" cellpadding="6" cellspacing="0">
  <tr><td><b>Transaction ID</b></td><td>{esc(row['transaction_id'])}</td></tr>
  <tr><td><b>Transaction Time</b></td><td>{esc(row['transaction_timestamp'])}</td></tr>
  <tr><td><b>Amount</b></td><td>{esc(row['transaction_amount'])} {esc(row['currency'])}</td></tr>
  <tr><td><b>Your Limit</b></td><td>{esc(row['transaction_limit'])}</td></tr>
  <tr><td><b>Merchant</b></td><td>{esc(row['merchant_name'])}</td></tr>
  <tr><td><b>Location</b></td><td>{esc(row['city'])}, {esc(row['country'])}</td></tr>
</table>
<br>
<p>Thanks<br><b>FinGuard Support Team</b></p>
</body>
</html>
"""
    return subject, body


def send_email(to_email, subject, body):
    # TODO: paste your email sending code here (uses to_email, subject, body)
    raise NotImplementedError("Add email sending logic")


@dp.foreach_batch_sink(name="high_value_transactions_email_sink")
def high_value_transactions_email_sink(df, batch_id):
    alerts = (
        df.filter(F.col("customer_email").isNotNull())
        .dropDuplicates(["alert_id"])
        .collect()
    )
    for row in alerts:
        subject, body = build_alert_email(row)
        send_email(row["customer_email"], subject, body)
    return


@dp.append_flow(target="high_value_transactions_email_sink")
def high_value_transactions_email_flow():
    return spark.readStream.table("fineguard.gold.high_value_transactions_alert")
