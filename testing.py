from delta_rest_client import DeltaRestClient

delta_client = DeltaRestClient(
    base_url='https://api.delta.exchange',
    api_key='lbTYnaMuUuDRdgqhRqaw7tcqbK1ReU',
    api_secret='SNYGrzuvZYFMKeTbvAiHddBrL4PIQMmDockvMHKzIWjtOgjcynaCNXtBXMHl'
)

instrument_id = 2
leverage = 25

try:
    response = delta_client.set_leverage(instrument_id, leverage)
    print(response)
except Exception as e:
    print(f"Error: {e}")
