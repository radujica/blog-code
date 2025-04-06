import dagster as dg


# a string when across different code locations <- shown as external asset
@dg.asset(deps=["table2"])
def table42(context: dg.AssetExecutionContext) -> None:
    context.log.info("Hello, world too! the 3rd")


defs = dg.Definitions(assets=[table42])
