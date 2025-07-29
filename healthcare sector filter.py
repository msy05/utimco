import csv

def load_csv_data(filename):
    """
    Load CSV data and organize it into a list of dictionaries
    """
    data = []
    skipped_rows = 0
    
    with open(filename, "r") as csvfile:
        reader = csv.DictReader(csvfile)  # This automatically uses the first row as headers
        
        # Print the column headers to make sure they match
        # print(f"Column headers found: {reader.fieldnames}")
        
        for row_num, row in enumerate(reader):
            # Convert numeric fields from strings to numbers
            try:
                # Handle empty strings and convert to float
                row['prccq'] = float(row['prccq']) if row['prccq'] and row['prccq'].strip() != '' else None
                row['epsf12'] = float(row['epsf12']) if row['epsf12'] and row['epsf12'].strip() != '' else None
            except (ValueError, KeyError) as e:
                skipped_rows += 1
                if skipped_rows <= 5:  # Show first 5 errors
                    print(f"Skipping row {row_num + 2}: {e}")
                continue
                
            data.append(row)
    
    print(f"Loaded {len(data)} records from {filename}")
    if skipped_rows > 0:
        print(f"Skipped {skipped_rows} rows due to data issues")
    
    # Show a sample of the data
    print(f"\nSample of first 3 records:")
    for i, row in enumerate(data[:3]):
        print(f"Row {i+1}: ticker={row.get('tic')}, quarter={row.get('datafqtr')}, price={row.get('prccq')}, eps_ttm={row.get('epsf12')}")
    
    return data

def find_stocks_with_good_pe(data, target_quarter):
    """
    Find stocks in a specific quarter with P/E ratio between 0 and 20
    Using epsf12 (EPS TTM excluding extraordinary items)
    
    Returns list of dictionaries with ticker, P/E ratio, price, and EPS
    """
    # Counters for debugging
    total_in_quarter = 0
    missing_price = 0
    missing_eps = 0
    zero_eps = 0
    negative_pe = 0
    high_pe = 0
    good_stocks = []
    
    print(f"\nAnalyzing data for {target_quarter} using epsf12 (TTM EPS)...")
    print("-" * 60)
    
    # Let's also track some sample P/E ratios to see what we're getting
    all_pe_ratios = []
    sample_data = []
    
    for row in data:
        # Check if this row is for our target quarter
        if row['datafqtr'] == target_quarter:
            total_in_quarter += 1
            
            # Store first 10 records for inspection
            if len(sample_data) < 10:
                sample_data.append({
                    'ticker': row['tic'],
                    'price': row['prccq'],
                    'eps_ttm': row['epsf12']
                })
            
            # Check for missing price data
            if row['prccq'] is None or row['prccq'] == '':
                missing_price += 1
                continue
                
            # Check for missing EPS data
            if row['epsf12'] is None or row['epsf12'] == '':
                missing_eps += 1
                continue
                
            # Check for zero EPS (can't calculate P/E)
            if row['epsf12'] == 0:
                zero_eps += 1
                continue
                
            # Calculate P/E ratio using TTM EPS
            pe_ratio = row['prccq'] / row['epsf12']
            all_pe_ratios.append(pe_ratio)
            
            # Check if P/E is negative (negative earnings)
            if pe_ratio <= 0:
                negative_pe += 1
                continue
                
            # Check if P/E is too high
            if pe_ratio >= 20:
                high_pe += 1
                continue
                
            # If we get here, it's a good stock!
            good_stocks.append({
                'ticker': row['tic'],
                'pe_ratio': pe_ratio,
                'price': row['prccq'],
                'eps_ttm': row['epsf12']
            })
    
    # Sort by P/E ratio (lowest first)
    good_stocks.sort(key=lambda x: x['pe_ratio'])
    
    # Print detailed breakdown
    print(f"Total stocks in {target_quarter}: {total_in_quarter}")
    print(f"Missing price data: {missing_price}")
    print(f"Missing TTM EPS data: {missing_eps}")
    print(f"Zero TTM EPS (can't calculate P/E): {zero_eps}")
    print(f"Negative P/E (negative earnings): {negative_pe}")
    print(f"P/E >= 20 (too high): {high_pe}")
    print(f"Valid stocks with 0 < P/E < 20: {len(good_stocks)}")
    
    # Show sample data for debugging
    print(f"\nSample of data in {target_quarter} (using TTM EPS):")
    for i, sample in enumerate(sample_data[:5]):
        pe = "N/A"
        if sample['price'] is not None and sample['eps_ttm'] is not None and sample['eps_ttm'] != 0:
            pe = f"{sample['price'] / sample['eps_ttm']:.2f}"
        print(f"  {sample['ticker']}: Price={sample['price']}, TTM_EPS={sample['eps_ttm']}, P/E={pe}")
    
    # Show P/E ratio distribution
    if all_pe_ratios:
        all_pe_ratios.sort()
        positive_ratios = [pe for pe in all_pe_ratios if pe > 0]
        if positive_ratios:
            print(f"\nP/E ratio distribution (positive ratios only):")
            print(f"  Lowest P/E: {min(positive_ratios):.2f}")
            print(f"  Highest P/E: {max(positive_ratios):.2f}")
            print(f"  Number with P/E < 10: {len([pe for pe in positive_ratios if pe < 10])}")
            print(f"  Number with P/E 10-20: {len([pe for pe in positive_ratios if 10 <= pe < 20])}")
            print(f"  Number with P/E 20-30: {len([pe for pe in positive_ratios if 20 <= pe < 30])}")
            print(f"  Number with P/E 30-50: {len([pe for pe in positive_ratios if 30 <= pe < 50])}")
            print(f"  Number with P/E > 50: {len([pe for pe in positive_ratios if pe >= 50])}")
    
    return good_stocks

def calculate_peg_for_stock(data, ticker, current_quarter):
    """
    Calculate PEG ratio for a specific stock using TTM EPS data
    """
    # Find current quarter data for this ticker
    current_data = None
    for row in data:
        if row['tic'] == ticker and row['datafqtr'] == current_quarter:
            current_data = row
            break
    
    if not current_data:
        return f"No data found for {ticker} in {current_quarter}"
    
    # Calculate current P/E using TTM EPS
    if current_data['epsf12'] == 0 or current_data['epsf12'] is None:
        return f"Cannot calculate P/E for {ticker} - TTM EPS is zero or missing"
    
    current_pe = current_data['prccq'] / current_data['epsf12']
    
    # Figure out the previous year's same quarter
    # e.g., if current is 2016Q1, we want 2015Q1
    year = int(current_quarter[:4])
    quarter_part = current_quarter[4:]  # Q1, Q2, etc.
    previous_quarter = f"{year-1}{quarter_part}"
    
    # Find previous year data
    previous_data = None
    for row in data:
        if row['tic'] == ticker and row['datafqtr'] == previous_quarter:
            previous_data = row
            break
    
    if not previous_data:
        return f"No data found for {ticker} in {previous_quarter}"
    
    # Calculate EPS growth using TTM EPS
    current_eps = current_data['epsf12']
    previous_eps = previous_data['epsf12']
    
    if previous_eps <= 0 or previous_eps is None:
        return f"Cannot calculate growth for {ticker} - previous TTM EPS is {previous_eps}"
    
    # EPS growth rate as percentage
    eps_growth = ((current_eps - previous_eps) / previous_eps) * 100
    
    # Calculate PEG ratio
    if eps_growth <= 0:
        peg_ratio = "Undefined (negative growth)"
    else:
        peg_ratio = current_pe / eps_growth
    
    return {
        'ticker': ticker,
        'current_quarter': current_quarter,
        'previous_quarter': previous_quarter,
        'current_eps_ttm': current_eps,
        'previous_eps_ttm': previous_eps,
        'eps_growth_percent': eps_growth,
        'pe_ratio': current_pe,
        'peg_ratio': peg_ratio
    }

def analyze_quarter(data, quarter):
    """
    Complete analysis: find good P/E stocks and calculate PEG ratios
    """
    print(f"\n" + "="*50)
    print(f"ANALYZING QUARTER: {quarter}")
    print(f"="*50)
    
    # Step 1: Find stocks with good P/E ratios
    good_pe_stocks = find_stocks_with_good_pe(data, quarter)
    
    if not good_pe_stocks:
        print("No stocks found meeting P/E criteria")
        return [], []  # Return empty lists instead of None
    
    # Show ALL stocks by P/E
    print(f"\nAll stocks with P/E < 20:")
    print(f"{'Ticker':<8} {'P/E Ratio':<10} {'Price':<10} {'TTM_EPS':<10}")
    print("-" * 42)
    
    for stock in good_pe_stocks:
        print(f"{stock['ticker']:<8} {stock['pe_ratio']:<10.2f} {stock['price']:<10.2f} {stock['eps_ttm']:<10.2f}")
    
    # Step 2: Calculate PEG ratios for all stocks that match criteria
    print(f"\nCalculating PEG ratios for stocks...")
    print("-" * 60)
    good_peg_tickers = []

    for stock in good_pe_stocks:
        ticker = stock['ticker']
        # print(f"\nAnalyzing {ticker}:")
        
        peg_result = calculate_peg_for_stock(data, ticker, quarter)
        
        if isinstance(peg_result, str):  # Error message
            print(f"  Error: {peg_result}")
        else:
            # print(f"  P/E Ratio: {peg_result['pe_ratio']:.2f}")
            # print(f"  EPS Growth: {peg_result['eps_growth_percent']:.1f}%")
            # print(f"  PEG Ratio: {peg_result['peg_ratio']}")
            
            GOOD_PEG = 1
            if not isinstance(peg_result['peg_ratio'], str) and peg_result['peg_ratio'] < GOOD_PEG:
                #print(f"  {ticker} has a good PEG ratio: {peg_result['peg_ratio']:.2f}")
                good_peg_tickers.append(ticker)
    print(f"\nStocks with good PEG ratios (PEG < 1):", end=' ')
    for ticker in good_peg_tickers:
        print(f"{ticker}, ", end='')
    print()  # Newline for better formatting
    return good_pe_stocks, good_peg_tickers



# Main execution
if __name__ == "__main__":
    # Load your data (update filename as needed)
    print("Loading healthcare.csv...")
    
    try:
        data = load_csv_data("healthcare.csv")
        
        # Analyze a specific quarter
        target_quarter = "2016Q1"  # Change this to whatever quarter you want
        pe_stocks, peg_stocks = analyze_quarter(data, target_quarter)
        
        # You can also run individual functions:
        # good_stocks = find_stocks_with_good_pe(data, "2016Q1")
        # peg_data = calculate_peg_for_stock(data, "JNJ", "2016Q1")
        
    
    except Exception as e:
        print(f"An error occurred: {e}")