from pandas import DataFrame

def drawLineInConsole(data: DataFrame):
    """Draws a matrix-style table in the console with both row indices and column headers.

    Args:
        data (DataFrame): The matrix-like DataFrame to display
    """
    if data.empty:
        print("DataFrame is empty.")
        return

    # Convert everything to string to simplify sizing
    data_str = data.astype(str)

    # Compute max widths
    col_widths = [max(len(str(col)), max(data_str[col].map(len))) for col in data.columns]
    index_width = max(len(str(i)) for i in data.index)

    # Format helpers
    def row_line(corner_left, connector, corner_right):
        return (
            corner_left +
            connector.join("─" * (w + 2) for w in [index_width] + col_widths) +
            corner_right
        )

    def format_row(index_val, row):
        row_items = [str(index_val).ljust(index_width)] + [
            str(cell).ljust(w) for cell, w in zip(row, col_widths)
        ]
        return "│ " + " │ ".join(row_items) + " │"

    # Build parts
    top = row_line("┌", "┬", "┐")
    mid = row_line("├", "┼", "┤")
    bot = row_line("└", "┴", "┘")

    # Header row
    header_row = format_row("", data.columns)

    # Print matrix
    print(top)
    print(header_row)
    print(mid)
    for idx in data.index:
        print(format_row(idx, data_str.loc[idx]))
    print(bot)
