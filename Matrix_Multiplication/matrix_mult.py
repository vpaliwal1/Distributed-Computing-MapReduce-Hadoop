from mrjob.job import MRJob
from mrjob.step import MRStep

M_rows = -1
M_cols = 0
N_rows = -1
N_cols = 0


class MR_MatrixMult(MRJob):
    file = open('Output_mult.txt', 'w')
    def steps(self):
        return [MRStep(mapper_raw=self.mapper_one,
                       reducer=self.reducer_one),
                MRStep(mapper=self.mapper_two,
                       reducer=self.reducer_two)]
    def mapper_one(self,input_path, input_uri):
        global M_rows,M_cols, N_cols,N_rows
        filename = open(input_path,"r")

        # i,j, val = line.split()
        # filename = os.environ['map_input_file']
        #
        # print(filename)
        for i in filename:
            data = i.split()
            y = len(data)
            if 'A' in input_path:
                if M_cols==0:
                    M_cols= y-1
                M_rows = M_rows +1
                for j in range(y):
                    yield j, (0, M_rows, data[j])
            else:
                if N_cols == 0:
                    N_cols = y - 1
                N_rows = N_rows+1
                for j in range(y):
                    yield N_rows, (1, j, data[j])

    def reducer_one(self, _, values):
        list1 = []
        list2 = []

        for val in values:
            if val[0] == 0:
                list1.append(val)
            if val[0] ==1:
                list2.append(val)

        for i in list1:
            for j in list2:
                yield (i[1],j[1]),float(i[2])*float(j[2])

    def mapper_two(self, j, value):
        yield (j),value

    def reducer_two(self, key, value):
        total = sum(value)
        yield key, total
        self.file.write(str(total) + "\n")




if __name__ == '__main__':

    MR_MatrixMult.run()
