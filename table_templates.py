
class TableTemplate:

    def __init__(self, number_start, number_end, percent_start, percent_end, jump_val):
        self.number_start = number_start
        self.number_end = number_end
        self.percent_start = percent_start
        self.percent_end = percent_end
        self.jump_val = jump_val

    def get_table_data(self, raw_data):
        years = {}
        year = 2009
        rows = []
        rows_finished = False

        for i in range(4):
            years[year] = {}
            years[year]["number"], years[year]["percent_of_total"] = self.get_totals(numbers=raw_data[number_start:number_end], percents=raw_data[percent_start:percent_end])
            self.number_start += self.jump_val
            self.number_end += self.jump_val
            self.percent_start += self.jump_val
            self.percent_end += self.jump_val
            year += 1

        row_index = percent_end + 2

        while not rows_finished:
            rows.append(raw_data[row_index])
            row_index += 1
            if raw_data[row_index] == "":
                rows_finished = True

        return (years, rows)

    def make_num(self, raw_val, type):
      if type == "p":
          return float(raw_val.strip("%"))
      return int(raw_val.replace(",", ""))


    def get_totals(self, **kwargs):
      nums = [make_num(num, 'i') for num in kwargs["numbers"]]
      if kwargs["percents"] is not None and kwargs["percents"] is not "":
          percs = [make_num(num, 'p') for num in kwargs["percents"]]
      return (nums, percs)
