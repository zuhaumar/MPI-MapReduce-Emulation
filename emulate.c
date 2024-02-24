#include<stdio.h>
#include<math.h>
#include<stdlib.h>
#include<mpi.h>
#include<string.h>

int arrSize=8;  //2^4
int rows_per_mapper=0;
int *mapper_output=NULL;
int rows;    //rows of mapper's output array
int cols;    //cols of mapper's output array

//Function Prototypes
void load_matrix_from_file(int**,char[]);
void load_key_values_from_file(int*,int,int,char[]);
void print_matrix(int**,int,int);
void print_2Dmatrix(int*,int,int);
void mapper(int*,int*,int);
int serial_execution(int**,int**,int*, int *);
void matrix_to_file(int *arr,int rows,int cols,char[]);
void print_key_value(int*, int, int);
void reducer(int *, int , int, int *);
int main(int argc,char** argv)
{
    //Local Variables
    char fileName[]="matrix.txt";
    char outputFile[]="keyVal.txt";
    int **matrixA=NULL;
    int **matrixB=NULL;
    int *subArr=NULL;
    int *sendB=NULL;
    int *serial_output=NULL, *comp = NULL;
    int *gather=NULL;
    int *receive=NULL;
    int *sum = NULL;
    int *final = NULL;
    int rank, comm_size;
    int mappers,reducers,usedReducersRank;
    int disp;
    int recSize;
	
    arrSize=atoi(argv[1]);
    mappers=atoi(argv[2]);
    reducers=atoi(argv[3]);

    //Check if number of mappers is greater than number of reducers
    if(reducers>mappers)
    {
        printf("Number of mappers must be greater than number of reducers\n");
        printf("Exiting . . .\n");
        return 1;
    }

    //Calculating number of rows that will be sent to each mapper/mpi process
    rows_per_mapper=arrSize/mappers;

    MPI_Status status;
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    if(reducers%2==0){
        usedReducersRank=mappers+reducers;
    }
    else{
        usedReducersRank=mappers+(reducers-1);
    }

    if(rank>=1 && rank<=mappers)
    {
        subArr = (int*)malloc(sizeof(int) * rows_per_mapper * arrSize);
        for (int i = 0; i < rows_per_mapper; i++) 
        {
            for (int j = 0; j < arrSize; j++) 
            {
                int index = i * arrSize + j;
                subArr[index]=0;
            }
        }

        sendB = (int*)malloc(sizeof(int) * arrSize * arrSize);
        for (int i = 0; i < arrSize; i++) 
        {
            for (int j = 0; j < arrSize; j++) 
            {
                int index = i * arrSize + j;
                sendB[index]=0;
            }
        }

        rows=rows_per_mapper*arrSize;
        cols = arrSize+2;
        mapper_output = (int*)malloc(sizeof(int) * rows * cols);
        for (int i = 0; i < rows; i++) 
        {
            for (int j = 0; j < cols; j++) 
            {
                int index = i * cols + j;
                mapper_output[index]=0;
            }
        }
    }

    //Master
    if(rank==0)
    {	
    	int len;
    	char name[MPI_MAX_PROCESSOR_NAME];
    	MPI_Get_processor_name(name,&len);
    	printf("Master with process_id %d running on %s\n",rank,name);
        //Dynamic memory allocation
        matrixA=(int**)malloc(arrSize*sizeof(int*));
        for(int i=0;i<arrSize;i++)
        {
            matrixA[i]=(int*)malloc(arrSize*sizeof(int));
        }

        matrixB=(int**)malloc(arrSize*sizeof(int*));
        for(int i=0;i<arrSize;i++)
        {
            matrixB[i]=(int*)malloc(arrSize*sizeof(int));
        }

        subArr = (int*)malloc(sizeof(int) * rows_per_mapper * arrSize);
        for (int i = 0; i < rows_per_mapper; i++) 
        {
            for (int j = 0; j < arrSize; j++) 
            {
                int index = i * arrSize + j;
                subArr[index]=0;
            }
        }

        sendB = (int*)malloc(sizeof(int) * arrSize * arrSize);
        for (int i = 0; i < arrSize; i++) 
        {
            for (int j = 0; j < arrSize; j++) 
            {
                int index = i * arrSize + j;
                sendB[index]=0;
            }
        }

        serial_output = (int*)malloc(sizeof(int) * arrSize * arrSize);
        for (int i = 0; i < arrSize; i++) 
        {
            for (int j = 0; j < arrSize; j++) 
            {
                int index = i * arrSize + j;
                serial_output[index]=0;
            }
        }

        comp = (int*)malloc(sizeof(int) * arrSize * arrSize);
        for (int i = 0; i < arrSize; i++) 
        {
            for (int j = 0; j < arrSize; j++) 
            {
                int index = i * arrSize + j;
                comp[index]=0;
            }
        }

        //Reading matrix from file
        load_matrix_from_file(matrixA,fileName);
        load_matrix_from_file(matrixB,fileName);

        //Flatttening matrix B to 1D matrix
        int row = 0;
        for(int i=0;i<arrSize;i++)
        {
            for(int j=0;j<arrSize;j++)
            {
                sendB[row++] = matrixB[i][j];
            }
        }

        row = 0;
        disp = 0;
        for(int rank=1;rank<=mappers;rank++)
        {
            for(int i=0;i<rows_per_mapper;i++)
            {
                for(int j=0;j<arrSize;j++)
                {
                    //Flattening 2D into 1D
                    int index = i * arrSize + j;
                    subArr[index] = matrixA[row][j];
                }
                row++;
            }
            //Sending the subArr to mapper processes
            MPI_Send(&subArr[0], rows_per_mapper * arrSize, MPI_INT, rank, 0, MPI_COMM_WORLD);

            //Sending row displacement to mapper processes
            MPI_Send(&disp, 1, MPI_INT, rank, 0, MPI_COMM_WORLD);
            disp+=rows_per_mapper;
        }

        printf("\n");

        //Sending matrix B to mapper processes
        for(int rank=1;rank<=mappers;rank++)
        {
        	printf("Task Map assigned to process %d\n",rank);
            MPI_Send(&sendB[0], arrSize * arrSize, MPI_INT, rank, 0, MPI_COMM_WORLD);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    if(rank>=1 && rank<=mappers)
    {
        MPI_Recv(&subArr[0], rows_per_mapper * arrSize, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&disp, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&sendB[0], arrSize * arrSize, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //Calling mapper function
        mapper(subArr,sendB,disp);
        printf("Process %d has completed task Map\n",rank);
        // print_key_value(mapper_output,rows,cols);
        // printf("\n");
        matrix_to_file(mapper_output,rows,cols,outputFile);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    if(rank == 0)
    {
        //Calculating number of rows and cols
        rows=(rows_per_mapper*arrSize*mappers);
        cols = arrSize+2;

        //Dynamic Memory Allocation
        gather = (int*)malloc(sizeof(int) * rows * cols);
        for (int i = 0; i < rows; i++) 
        {
            for (int j = 0; j < cols; j++) 
            {
                int index = i * arrSize + j;
                gather[index]=0;
            }
        }

        load_key_values_from_file(gather,rows,cols,outputFile);
  
        for(int rank=mappers+1;rank<=usedReducersRank;rank++)
        {
            MPI_Send(&rows, 1, MPI_INT, rank, 0, MPI_COMM_WORLD);
            MPI_Send(&cols, 1, MPI_INT, rank, 0, MPI_COMM_WORLD);
        }

		if(reducers%2==0)
        recSize=rows/reducers;
        else
        recSize=rows/(reducers-1);

        receive = (int*)malloc(sizeof(int) * recSize *  cols);
        for (int i = 0; i < recSize; i++) 
        {
            for (int j = 0; j < cols; j++) 
            {
                int index = i * cols + j;
                receive[index]=0;
            }
        }
        
        int allRows = 0;
        for(int rank=mappers+1;rank<=usedReducersRank;rank++)
        {
            for(int i=0;i<recSize;i++)
            {
                for(int j=0;j<cols;j++)
                {
                    int index = i * cols + j;
                    receive[index] = gather[allRows * cols + j];
                }
                allRows++;
            }
            printf("Task Reduce assigned to process %d\n",rank);
            //printf("Task Reduce assigned to process %d",7);
            MPI_Send(&receive[0], recSize * cols, MPI_INT, rank, 0, MPI_COMM_WORLD);
        }
    }

    if(rank>mappers && rank<=usedReducersRank)
    {
        MPI_Recv(&rows,1 , MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&cols, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        if(reducers%2==0)
        recSize=rows/reducers;
        else
        recSize=rows/(reducers-1);

        receive = (int*)malloc(sizeof(int) * recSize *  cols);
        for (int i = 0; i < recSize; i++) 
        {
            for (int j = 0; j < cols; j++) 
            {
                int index = i * cols + j;
                receive[index]=0;
            }
        }
        MPI_Recv(&receive[0], recSize * cols, MPI_INT, 0, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        int len;
    	char name[MPI_MAX_PROCESSOR_NAME];
    	MPI_Get_processor_name(name,&len);
        printf("Process %d received task Reduce on %s\n",rank,name);
        sum = (int*)malloc(sizeof(int) * rows * 3);
        for (int i = 0; i < rows; i++) 
        {
            for (int j = 0; j < 3; j++) 
            {
                int index = i * 3 + j;
                sum[index]=0;
            }
        }
        reducer(receive,recSize,cols,sum);
        printf("Process %d has completed task Reduce\n",rank);
    }

	MPI_Barrier(MPI_COMM_WORLD);
	if (rank == 0)
	{
        sum = (int*)malloc(sizeof(int) * rows * 3);
        for (int i = 0; i < rows; i++) 
        {
            for (int j = 0; j < 3; j++) 
            {
                int index = i * 3 + j;
                sum[index]=0;
            }
        }
        
        load_key_values_from_file(sum,rows,3,"final.txt");
        
        final = (int*)malloc(sizeof(int) * arrSize * arrSize);
        for (int i = 0; i < arrSize; i++) 
        {
            for (int j = 0; j < arrSize; j++) 
            {
                int index = i * arrSize + j;
                final[index]=0;
            }
        }
        
        for (int i = 0; i < rows; i++) {
            int row = sum[i * 3 + 0];
            int col = sum[i * 3 + 1];
            int val =sum[i * 3 + 2];
            int index = row * arrSize + col;  
            final[index] = val;
        }
        
        printf("Job has been completed!\n");
        int finalans = serial_execution(matrixA,matrixB,serial_output,final);
        
        if (finalans == 1)
        {
        	printf("Matrix comparison function returned: True\n");
        }
        
        else if (finalans == 0)
        {
        	printf("Matrix comparison function returned: False\n");
        }
        matrix_to_file(final,arrSize,arrSize,"result.txt");
	}


    //Clean-up
    if(rank>=1 && rank<=mappers)
    {
        free(sendB);
        free(subArr);
        free(mapper_output);
    }

    if(rank == 0)
    {
        for(int i=0;i<arrSize;i++) 
        {
            free(matrixA[i]);
            free(matrixB[i]);
        }
        free(matrixA);
        free(matrixB);
        free(sendB);
        free(subArr);
        free(serial_output);
        free(gather);
        free(receive);
        free(sum);
        free(final);
    }

    if(rank>mappers && rank<comm_size)
    {
        free(receive);
        free(sum);
    }

    MPI_Finalize();
    return 0;
}

/*
Reads data from the file and saves the data in a 2D array
*/
void load_matrix_from_file(int**arr, char fileName[])
{
    FILE *file_pointer=fopen(fileName, "r");
    if(file_pointer!=NULL)
    {
        for(int i=0;i<arrSize;i++) 
        {
            for(int j=0;j<arrSize;j++) 
            {
                fscanf(file_pointer,"%d",&arr[i][j]);
            }
        }
        fclose(file_pointer);
    }
    else
    {
        printf("Error opening %s\n",fileName);
    }
}

/*
Prints a 1D matrix as 2D matrix
*/
void print_matrix(int**arr,int rows,int cols)
{
    for(int i=0;i<rows;i++)
    {
        for(int j=0;j<cols;j++)
        {
            printf("%d ",arr[i][j]);
        }
        printf("\n");
    }
}

void print_2Dmatrix(int*arr,int rows_per_mapper,int arrSize)
{

    for (int i = 0; i < rows_per_mapper; i++) 
    {
        for (int j = 0; j < arrSize; j++) 
        {
            int index = i * arrSize + j;
            printf("%d ",arr[index]);
        }
        printf("\n");
    }
}

/*
Input Key-Value pair: 
    -> Key: Starting row index of the block in matrix A
    -> Value: The actual block
Ouput Key-Value pair:
    -> Key: indices of matrix C where the sub-matrix calculated will be stored
    -> Values: sub-matrix
*/
void mapper(int*value,int* matrixB, int key)
{
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	int len;
    char name[MPI_MAX_PROCESSOR_NAME];
    MPI_Get_processor_name(name,&len);
	printf("Process %d received task Map on %s\n",rank,name);
    int rowI=0,colI;
    for (int i = 0; i < rows_per_mapper; i++)
    {
        for (int j = 0; j < arrSize; j++) 
        {
            colI=2;
            mapper_output[rowI * cols + 0] = key;
            mapper_output[rowI * cols + 1] = j;
            for (int k = 0; k < arrSize; k++) 
            {
                int sum =  value[i * arrSize + k] * matrixB[k * arrSize + j];
                //printf("%d, %d: %d\n",i,j,sum);
                mapper_output[rowI * cols + colI] = sum;
                colI++;
            }
            rowI++;
        }
        key++;
    }
}

void reducer(int *value, int rows, int cols, int * sum)
{

        for (int i = 0; i < rows; i++) 
        {
        	int s = 0;
            for (int j = 0; j < cols; j++) 
            {
            	if (j==0 || j == 1)
            	{
            	sum[i * 3 + j] = value[i *cols + j];
            	}
            	else{
                int temp_sum = value[i *cols + j]; 
                s+=temp_sum;
                sum[i * 3 + 2] = s;
                }
            }
        }

	   matrix_to_file(sum,rows,3,"final.txt");
}
int serial_execution(int **matrixA,int **matrixB, int* matrixC, int *comp_matrix){

    //Set to zero if the two matrices are not equal
    int flag = 1;   
    for (int i = 0; i < arrSize; i++)
    {
        for (int j = 0; j < arrSize; j++) 
        {
            int sum = 0;
            for (int k = 0; k < arrSize; k++) 
            {
                sum += matrixA[i][k] * matrixB[k][j];
            }
            matrixC[i * arrSize + j] = sum;
        }
    }
    //Comparing the two results
    for (int i=0; i<arrSize; i++) 
    {
        for (int j=0; j<arrSize; j++)
         {
            if (*(matrixC + i * arrSize + j) != *(comp_matrix+ i * arrSize + j))
            {
                flag = 0;
                break;
            }
        }
    }
    return flag;
}

void matrix_to_file(int *arr,int rows,int cols,char fileName[]){

   FILE *file = fopen(fileName, "a"); 
   if (file == NULL) 
   { 
      printf("Error: Could not open file for writing\n");
      return;
   }

   for (int i = 0; i < rows; i++) 
   {
      for (int j = 0; j < cols; j++) 
      {
         fprintf(file, "%d ", arr[i * cols +j]);
      }
      fprintf(file, "\n"); 
   }

   fclose(file);
}

void print_key_value(int *output,int rows,int cols)
{
    for (int i = 0; i < rows; i++) 
    {
        for (int j = 0; j < cols; j++) 
        {
            int index = i * cols + j;
            if(j==0)
                printf("%d, ",output[index]);
            else if(j==1)
                printf("%d: ",output[index]);
            else
                printf("%d ",output[index]);
        }
        printf("\n");
    }
}

void load_key_values_from_file(int* gather, int rows, int cols, char fileName[]){

    FILE *file_pointer=fopen(fileName, "r");
    if(file_pointer!=NULL)
    {
        for(int i=0;i<rows;i++) 
        {
            for(int j=0;j<cols;j++) 
            {
                int temp;
                fscanf(file_pointer,"%d", &gather[i*cols+j]);
            }
        }
        fclose(file_pointer);
    }
    else
    {
        printf("Error opening %s\n",fileName);
    }

}
