#include<stdio.h>
#include<math.h>
#include<stdlib.h>

int arrSize=16;  //2^4

void init_matrix(int**);
void print_matrix(int**);
void save_matrix_to_file(int**,char[]);

int main(int argc,char** argv) {

    int **arr=NULL;
    char fileName[50]="matrix.txt\0";
    arrSize=atoi(argv[1]);
    //Dynamic memmory allocation
    arr=(int**)malloc(arrSize*sizeof(int*));
    for (int i=0;i<arrSize;i++)
    {
        arr[i]=(int*)malloc(arrSize*sizeof(int));
    }

    //Initializing array
    init_matrix(arr);
    //print_matrix(arr);

    //Writing array to file
    save_matrix_to_file(arr,fileName);

    //Clean-up
    for(int i=0; i<arrSize;i++) 
    {
        free(arr[i]);
    }
    free(arr);
    return 0;
}

void init_matrix(int**arr)
{
    for(int i=0;i<arrSize;i++)
    {
        for(int j=0;j<arrSize;j++)
        {
            arr[i][j]=rand()%9+1;
        }
    }
}

void print_matrix(int**arr)
{
    for(int i=0;i<arrSize;i++)
    {
        for(int j=0; j<arrSize;j++)
        {
            printf("%d ",arr[i][j]);
        }
        printf("\n");
    }
    printf("\n");
}

void save_matrix_to_file(int**arr,char fileName[])
{
    FILE *fp=fopen(fileName, "w");
    if(fp!=NULL)
    {
        for(int i=0;i<arrSize;i++) 
        {
            for(int j=0;j<arrSize;j++) 
            {
                fprintf(fp,"%d ",arr[i][j]); 
            }

            fprintf(fp, "\n"); 
        }
        fclose(fp);
        printf("Matrix written to %s\n", fileName);
    }
    else
    {
        printf("Error opening %s\n", fileName);
    }
}