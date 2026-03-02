import asyncio
import websockets
from nepse import AsyncNepse
import json
import logging
import os

# Import validation utilities
from validator import validate_stock_symbol, validate_index_name

# Import rate limiting
from rate_limiter import check_rate_limit

logger = logging.getLogger(__name__)

# Initialize Nepse Async
nepseAsync = AsyncNepse()
nepseAsync.setTLSVerification(False)

def _describe_payload(payload):
    try:
        if isinstance(payload, list):
            first = payload[0] if payload else None
            return f"list(len={len(payload)}, first={first})"
        return f"{type(payload).__name__}({str(payload)[:200]})"
    except Exception:
        return f"{type(payload).__name__}"

def _ensure_list_of_dicts(label: str, payload):
    if not isinstance(payload, list):
        return {"error": f"{label} expected list, got {type(payload).__name__}"}
    for i, item in enumerate(payload):
        if not isinstance(item, dict):
            return {"error": f"{label} item {i} expected dict, got {type(item).__name__}"}
    return {"ok": True}

# Common validation functions for WebSocket
def validate_stock_or_return_error(symbol: str):
    """Validate stock symbol and return error dict if invalid"""
    if not symbol:
        return {"error": "Symbol is required"}

    validation_result = validate_stock_symbol(symbol)
    if not validation_result["valid"]:
        error_msg = validation_result["error"]
        if validation_result.get("suggestions"):
            error_msg += f" Suggestions: {', '.join(validation_result['suggestions'])}"
        return {"error": error_msg}

    return {"valid": True, "symbol": validation_result["symbol"]}

def validate_index_or_return_error(index_name: str):
    """Validate index name and return error dict if invalid"""
    if not index_name:
        return {"error": "Index name is required"}

    validation_result = validate_index_name(index_name)
    if not validation_result["valid"]:
        error_msg = validation_result["error"]
        if validation_result.get("available_indices"):
            error_msg += f" Available indices: {', '.join(validation_result['available_indices'])}"
        return {"error": error_msg}

    return {"valid": True, "index_name": validation_result["index_name"]}

async def _get_summary():
    data = await nepseAsync.getSummary()
    check = _ensure_list_of_dicts("Summary", data)
    if "error" in check:
        logger.error("Summary payload shape error: %s | %s", check["error"], _describe_payload(data))
        return {"error": check["error"]}
    response = {obj["detail"]: obj["value"] for obj in data}
    return response

async def _get_nepse_index():
    data = await nepseAsync.getNepseIndex()
    check = _ensure_list_of_dicts("NepseIndex", data)
    if "error" in check:
        logger.error("NepseIndex payload shape error: %s | %s", check["error"], _describe_payload(data))
        return {"error": check["error"]}
    response = {obj["index"]: obj for obj in data}
    return response

async def _get_nepse_subindices():
    data = await nepseAsync.getNepseSubIndices()
    check = _ensure_list_of_dicts("NepseSubIndices", data)
    if "error" in check:
        logger.error("NepseSubIndices payload shape error: %s | %s", check["error"], _describe_payload(data))
        return {"error": check["error"]}
    response = {obj["index"]: obj for obj in data}
    return response

async def _get_trade_turnover_transaction_subindices():
    companies_raw = await nepseAsync.getCompanyList()
    check = _ensure_list_of_dicts("CompanyList", companies_raw)
    if "error" in check:
        logger.error("CompanyList payload shape error: %s | %s", check["error"], _describe_payload(companies_raw))
        return {"error": check["error"]}
    companies = {company["symbol"]: company for company in companies_raw}

    turnover_raw = await nepseAsync.getTopTenTurnoverScrips()
    check = _ensure_list_of_dicts("TopTenTurnoverScrips", turnover_raw)
    if "error" in check:
        logger.error("TopTenTurnoverScrips payload shape error: %s | %s", check["error"], _describe_payload(turnover_raw))
        return {"error": check["error"]}
    turnover = {obj["symbol"]: obj for obj in turnover_raw}

    transaction_raw = await nepseAsync.getTopTenTransactionScrips()
    check = _ensure_list_of_dicts("TopTenTransactionScrips", transaction_raw)
    if "error" in check:
        logger.error("TopTenTransactionScrips payload shape error: %s | %s", check["error"], _describe_payload(transaction_raw))
        return {"error": check["error"]}
    transaction = {obj["symbol"]: obj for obj in transaction_raw}

    trade_raw = await nepseAsync.getTopTenTradeScrips()
    check = _ensure_list_of_dicts("TopTenTradeScrips", trade_raw)
    if "error" in check:
        logger.error("TopTenTradeScrips payload shape error: %s | %s", check["error"], _describe_payload(trade_raw))
        return {"error": check["error"]}
    trade = {obj["symbol"]: obj for obj in trade_raw}

    gainers_raw = await nepseAsync.getTopGainers()
    check = _ensure_list_of_dicts("TopGainers", gainers_raw)
    if "error" in check:
        logger.error("TopGainers payload shape error: %s | %s", check["error"], _describe_payload(gainers_raw))
        return {"error": check["error"]}
    gainers = {obj["symbol"]: obj for obj in gainers_raw}

    losers_raw = await nepseAsync.getTopLosers()
    check = _ensure_list_of_dicts("TopLosers", losers_raw)
    if "error" in check:
        logger.error("TopLosers payload shape error: %s | %s", check["error"], _describe_payload(losers_raw))
        return {"error": check["error"]}
    losers = {obj["symbol"]: obj for obj in losers_raw}

    price_vol_raw = await nepseAsync.getPriceVolume()
    check = _ensure_list_of_dicts("PriceVolume", price_vol_raw)
    if "error" in check:
        logger.error("PriceVolume payload shape error: %s | %s", check["error"], _describe_payload(price_vol_raw))
        return {"error": check["error"]}
    price_vol_info = {obj["symbol"]: obj for obj in price_vol_raw}
    sector_sub_indices = await _get_nepse_subindices()
    sector_mapper = {
        "Commercial Banks": "Banking SubIndex",
        "Development Banks": "Development Bank Index",
        "Finance": "Finance Index",
        "Hotels And Tourism": "Hotels And Tourism Index",
        "Hydro Power": "HydroPower Index",
        "Investment": "Investment Index",
        "Life Insurance": "Life Insurance",
        "Manufacturing And Processing": "Manufacturing And Processing",
        "Microfinance": "Microfinance Index",
        "Mutual Fund": "Mutual Fund",
        "Non Life Insurance": "Non Life Insurance",
        "Others": "Others Index",
        "Tradings": "Trading Index",
    }

    scrips_details = {}
    for symbol, company in companies.items():
        company_details = {
            "symbol": symbol,
            "sector": company["sectorName"],
            "Turnover": turnover.get(symbol, {}).get("turnover", 0),
            "transaction": transaction.get(symbol, {}).get("totalTrades", 0),
            "volume": trade.get(symbol, {}).get("shareTraded", 0),
            "previousClose": price_vol_info.get(symbol, {}).get("previousClose", 0),
            "lastUpdatedDateTime": price_vol_info.get(symbol, {}).get("lastUpdatedDateTime", 0),
            "name": company.get("securityName", ""),
            "category": company.get("instrumentType"),
        }

        if symbol in gainers:
            company_details.update({
                "pointChange": gainers[symbol]["pointChange"],
                "percentageChange": gainers[symbol]["percentageChange"],
                "ltp": gainers[symbol]["ltp"],
            })
        elif symbol in losers:
            company_details.update({
                "pointChange": losers[symbol]["pointChange"],
                "percentageChange": losers[symbol]["percentageChange"],
                "ltp": losers[symbol]["ltp"],
            })
        else:
            company_details.update({
                "pointChange": 0,
                "percentageChange": 0,
                "ltp": 0,
            })

        if company_details["ltp"] == 0 or company_details["previousClose"] == 0:
            continue
        scrips_details[symbol] = company_details

    sector_details = {}
    sectors = {company["sectorName"] for company in companies.values()}
    for sector in sectors:
        total_trades, total_trade_quantity, total_turnover = 0, 0, 0
        for scrip_details in scrips_details.values():
            if scrip_details["sector"] == sector:
                total_trades += scrip_details["transaction"]
                total_trade_quantity += scrip_details["volume"]
                total_turnover += scrip_details["Turnover"]

        sector_details[sector] = {
            "transaction": total_trades,
            "volume": total_trade_quantity,
            "totalTurnover": total_turnover,
            "turnover": sector_sub_indices[sector_mapper.get(sector, sector)],
            "sectorName": sector,
        }

    return {"scripsDetails": scrips_details, "sectorsDetails": sector_details}

# WebSocket handler
async def handle_route(route: str, params: dict):
    # Routes that require symbol validation
    symbol_routes = ["DailyScripPriceGraph", "CompanyDetails", "PriceVolumeHistory", "FloorsheetOf"]

    # Validate symbol if route requires it
    if route in symbol_routes:
        symbol = params.get("symbol")
        validation_result = validate_stock_or_return_error(symbol)
        if "error" in validation_result:
            return validation_result
        # Update params with validated symbol
        params = {**params, "symbol": validation_result["symbol"]}

    route_handlers = {
        "Summary": lambda: _get_summary(),
        "NepseIndex": lambda: _get_nepse_index(),
        "LiveMarket": lambda: nepseAsync.getLiveMarket(),
        "TopTenTradeScrips": lambda: nepseAsync.getTopTenTradeScrips(),
        "TopTenTransactionScrips": lambda: nepseAsync.getTopTenTransactionScrips(),
        "TopTenTurnoverScrips": lambda: nepseAsync.getTopTenTurnoverScrips(),
        "TopGainers": lambda: nepseAsync.getTopGainers(),
        "TopLosers": lambda: nepseAsync.getTopLosers(),
        "IsNepseOpen": lambda: nepseAsync.isNepseOpen(),
        "DailyNepseIndexGraph": lambda: nepseAsync.getDailyNepseIndexGraph(),
        "DailySensitiveIndexGraph": lambda: nepseAsync.getDailySensitiveIndexGraph(),
        "DailyFloatIndexGraph": lambda: nepseAsync.getDailyFloatIndexGraph(),
        "DailySensitiveFloatIndexGraph": lambda: nepseAsync.getDailySensitiveFloatIndexGraph(),
        "DailyBankSubindexGraph": lambda: nepseAsync.getDailyBankSubindexGraph(),
        "DailyDevelopmentBankSubindexGraph": lambda: nepseAsync.getDailyDevelopmentBankSubindexGraph(),
        "DailyFinanceSubindexGraph": lambda: nepseAsync.getDailyFinanceSubindexGraph(),
        "DailyHotelTourismSubindexGraph": lambda: nepseAsync.getDailyHotelTourismSubindexGraph(),
        "DailyHydroPowerSubindexGraph": lambda: nepseAsync.getDailyHydroSubindexGraph(),
        "DailyInvestmentSubindexGraph": lambda: nepseAsync.getDailyInvestmentSubindexGraph(),
        "DailyLifeInsuranceSubindexGraph": lambda: nepseAsync.getDailyLifeInsuranceSubindexGraph(),
        "DailyManufacturingProcessingSubindexGraph": lambda: nepseAsync.getDailyManufacturingSubindexGraph(),
        "DailyMicrofinanceSubindexGraph": lambda: nepseAsync.getDailyMicrofinanceSubindexGraph(),
        "DailyMutualFundSubindexGraph": lambda: nepseAsync.getDailyMutualfundSubindexGraph(),
        "DailyNonLifeInsuranceSubindexGraph": lambda: nepseAsync.getDailyNonLifeInsuranceSubindexGraph(),
        "DailyOthersSubindexGraph": lambda: nepseAsync.getDailyOthersSubindexGraph(),
        "DailyTradingSubindexGraph": lambda: nepseAsync.getDailyTradingSubindexGraph(),
        "DailyScripPriceGraph": lambda: nepseAsync.getDailyScripPriceGraph(params.get("symbol")),
        "CompanyList": lambda: nepseAsync.getCompanyList(),
        "SectorScrips": lambda: nepseAsync.getSectorScrips(),
        "CompanyDetails": lambda: nepseAsync.getCompanyDetails(params.get("symbol")),
        "PriceVolume": lambda: nepseAsync.getPriceVolume(),
        "PriceVolumeHistory": lambda: nepseAsync.getCompanyPriceVolumeHistory(params.get("symbol")),
        "Floorsheet": lambda: nepseAsync.getFloorSheet(),
        "FloorsheetOf": lambda: nepseAsync.getFloorSheetOf(params.get("symbol")),
        "SecurityList": lambda: nepseAsync.getSecurityList(),
        "TradeTurnoverTransactionSubindices": lambda: _get_trade_turnover_transaction_subindices(),
        "SupplyDemand": lambda: nepseAsync.getSupplyDemand(),
        "NepseSubIndices": lambda: _get_nepse_subindices()
    }

    handler = route_handlers.get(route)
    if handler:
        return await handler()
    return {"error": "Route not found"}

# WebSocket listener
async def ws_listener(websocket, path=None):
    # Get client IP for rate limiting
    client_ip = websocket.remote_address[0] if websocket.remote_address else "unknown"

    try:
        async for message in websocket:
            try:
                # Check message rate limit
                allowed, info = check_rate_limit(client_ip, "websocket_message")
                if not allowed:
                    await websocket.send(json.dumps({
                        "error": "Rate limit exceeded for messages",
                        "route": request.get("route"),
                        "limit": info["limit"],
                        "remaining": info["remaining"],
                        "reset_time": info["reset_time"]
                    }))
                    continue

                # Parse the incoming message as JSON
                request = json.loads(message)
                route = request.get('route')
                params = request.get('params', {})
                message_id = request.get('messageId')

                # Handle the route
                response_data = await handle_route(route, params)

                # Structure response with messageId
                response = {
                    "messageId": message_id,
                    "route": route,
                    "data": response_data,
                    "rate_limit": {
                        "remaining": info["remaining"],
                        "limit": info["limit"],
                        "reset_time": info["reset_time"]
                    }
                }

                # Send the response back to the client
                await websocket.send(json.dumps(response))

            except json.JSONDecodeError:
                # Handle invalid JSON
                await websocket.send(json.dumps({"error": "Invalid JSON"}))
            except Exception as route_error:
                await websocket.send(json.dumps({
                    "messageId": message_id if 'message_id' in locals() else None,
                    "route": route if 'route' in locals() else None,
                    "error": str(route_error)
                }))

    except Exception as e:
        logger.error(f"WebSocket Error: {e}")
    finally:
        await websocket.close()

# Start WebSocket server on all interfaces
async def start_ws_server():
    port = int(os.getenv("PORT", "5555"))
    server = await websockets.serve(ws_listener, "0.0.0.0", port)
    print(f"WebSocket server started on ws://0.0.0.0:{port}")
    await server.wait_closed()

# Running the WebSocket server
if __name__ == "__main__":
    asyncio.run(start_ws_server())
