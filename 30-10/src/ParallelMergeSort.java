import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class ParallelMergeSort {
    public static void main(String[] args) {
        final int SIZE = 7000000;
        int[] list1 = new int[SIZE];

        for (int i = 0; i < list1.length; i++)
            list1[i] = (int)(Math.random() * 10000000);

        long time1 = System.currentTimeMillis();
        parallelMergeSort(list1);
        long time2 = System.currentTimeMillis();
        System.out.print(time2 - time1);
        for (int i:list1) {
            System.out.print(i+" ");
        }
    }

    public static void parallelMergeSort(int[] list) {
        RecursiveAction mainTask = new SortTask(list);
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(mainTask);
    }

    private static class SortTask extends RecursiveAction {
        private final int THRESHOLD = 500;
        private int[] list;

        SortTask(int[] list) {
            this.list = list;
        }

        @Override
        protected void compute() {
            if (list.length < THRESHOLD)
                Arrays.sort(list);
            else {
                int[] firstHalf = new int[list.length / 2];
                System.arraycopy(list, 0, firstHalf, 0, list.length / 2);

                int[] secondHalf = new int[list.length - list.length / 2];
                System.arraycopy(list, list.length / 2, secondHalf, 0, list.length - list.length / 2);

                invokeAll(new SortTask(firstHalf), new SortTask(secondHalf));

                MergeSort.merge(firstHalf, secondHalf, list);
            }
        }
    }

    private static class MergeSort {
        private static void merge(int[] list1, int[] list2, int[] temp) {
            int current1 = 0;
            int current2 = 0;
            int current3 = 0;

            while (current1 < list1.length && current2 < list2.length) {
                if (list1[current1] < list2[current2])
                    temp[current3++] = list1[current1++];
                else
                    temp[current3++] = list2[current2++];
            }

            while (current1 < list1.length)
                temp[current3++] = list1[current1++];

            while (current2 < list2.length)
                temp[current3++] = list2[current2++];
        }
    }
}
