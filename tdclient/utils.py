def rows2str(rows, num_rows=20):
    """
    Params:
        rows(list)
    Return:
        string of rows for printing
    """
    # check empty or not
    if not rows:
        return

    rows_list = []
    has_more_data = False
    for i, row in enumerate(rows):
        rows_list.append(row)
        if i >= num_rows:
            has_more_data = True
            break

    string_buffer = []
    num_column = len(rows_list[0])
    # Initilise the width of each column to a minimum value of `3`
    column_width = [3] * num_column
    # Compute the width of each column
    for row in rows_list[:num_rows]:
        for i in range(len(row)):
            column_width[i] = max(column_width[i], len(str(row[i])))

    # Create separate line
    sep_line = "+" + "+".join(map(lambda x: "-" * x, column_width)) + "+"
    string_buffer.append(sep_line)

    # data
    for row in rows_list[:num_rows]:
        data = []
        for i, cell in enumerate(row):
            data.append(str(cell).rjust(column_width[i]))
        data_str = "|" + "|".join(data) + "|"
        string_buffer.append(data_str)
    string_buffer.append(sep_line)
    if has_more_data:
        string_buffer.append("only showing top %d" % num_rows)

    return "\n".join(string_buffer)
