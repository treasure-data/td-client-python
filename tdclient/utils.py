def table2str(table, num_rows=20):
    """
    Params:
        table(list): table represented by list of list
    Return:
        string of table for printing
    """
    # check empty or not
    if not table:
        return

    num_rows += 1
    string_buffer = []
    num_column = len(table[0])
    # Initilise the width of each column to a minimum value of `3`
    column_width = [3] * num_column
    # Compute the width of each column
    for row in table[:num_rows]:
        for i in range(len(row)):
            column_width[i] = max(column_width[i], len(str(row[i])))

    # Create separate line
    sep_line = "+" + "+".join(map(lambda x: "-" * x, column_width)) + "+"
    # Column names
    column_names = []
    for i, cell in enumerate(table[0]):
        column_names.append(str(cell).rjust(column_width[i]))
    column_names_str = "|" + "|".join(column_names) + "|"
    string_buffer.append(sep_line)
    string_buffer.append(column_names_str)
    string_buffer.append(sep_line)

    # data
    for row in table[1:num_rows]:
        data = []
        for i, cell in enumerate(row):
            data.append(str(cell).rjust(column_width[i]))
        data_str = "|" + "|".join(data) + "|"
        string_buffer.append(data_str)
    string_buffer.append(sep_line)

    return "\n".join(string_buffer)
