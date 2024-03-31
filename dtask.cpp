#include <iostream>
#include <fstream>
#include <vector>
#include <queue>   // Include queue header
#include <mutex>  // Include mutex header
#include <condition_variable> // Include condition_variable header
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <thread>
#include <chrono>
#include <mpi.h>

using namespace std; // Add this line

mutex mutex1;
condition_variable producer_cv, consumer_cv;

// initializing number of threads for producers and consumers
int num_producers = 2;
int num_consumers = 2;

int hour_indicator = 48; // this value will be used to check if an hour has passed (48 rows for an hour)

int producer_count = 0; // producer counter
int consumer_count = 0; // consumer counter
int total_rows = 0; // total number of rows

// string variables and vectors to store data from the file
string row_index, time_stamp, traffic_light_id, number_of_cars;
vector<int> index_vector;
vector<int> traffic_light;
vector<int> car_counts;
vector<string> timestamps;

// struct for traffic data row
struct TrafficSignal {
    int row_index;
    string time_stamp;
    int light_id;
    int car_count;
};

// array to hold the totals of each of the 4 traffic lights
TrafficSignal traffic_signals[4] = {{0, "", 1, 0}, {0, "", 2, 0}, {0, "", 3, 0}, {0, "", 4, 0}};

// queue to store traffic light data
queue<TrafficSignal> traffic_queue;

// function to sort traffic light data
bool sort_method(TrafficSignal first, TrafficSignal second) {
    return first.car_count > second.car_count;
}

void producer_function(int rank) {
    while (producer_count < total_rows) {
        if (producer_count < total_rows) {
            traffic_queue.push({index_vector[producer_count], timestamps[producer_count], traffic_light[producer_count], car_counts[producer_count]});
            producer_count++;
        }
    }
}

void consumer_function(int rank) {
    while (consumer_count < total_rows) {
        if (!traffic_queue.empty()) {
            TrafficSignal signal = traffic_queue.front();

            // add the number of cars into the respective traffic light id
            for (int i = 0; i < 4; ++i) {
                if (signal.light_id == traffic_signals[i].light_id) {
                    traffic_signals[i].car_count += signal.car_count;
                    break;
                }
            }

            traffic_queue.pop(); // pop the data
            consumer_count++;
        }
        
        if (consumer_count % hour_indicator == 0) { // check if an hour has passed, checking every 48th row
            sort(traffic_signals, traffic_signals + 4, sort_method); // sorting data
            cout << "Traffic signals arranged on the basis of urgency | Time: " << traffic_queue.front().time_stamp << endl;
            cout << "------Traffic Light-------\t\t-----Number of Cars-----" << endl;
            for (int i = 0; i < 4; ++i) {
                cout << "\t" << traffic_signals[i].light_id << "\t\t\t\t\t" << traffic_signals[i].car_count << endl;
            }
        }
    }
}

// function to get data from file
void get_traffic_data() {
    ifstream input_file;

    string file_name;
    cout << "Enter the filename: ";
    cin >> file_name;

    input_file.open(file_name);

    if (input_file.is_open()) {
        string line;
        getline(input_file, line);

        while (!input_file.eof()) {
            getline(input_file, row_index, ',');
            index_vector.push_back(stoi(row_index));
            getline(input_file, time_stamp, ',');
            timestamps.push_back(time_stamp);
            getline(input_file, traffic_light_id, ',');
            traffic_light.push_back(stoi(traffic_light_id));
            getline(input_file, number_of_cars, '\n');
            car_counts.push_back(stoi(number_of_cars));

            total_rows += 1;
        }
        input_file.close();
    } else {
        cout << "Could not open file, please try again." << endl;
    }
}

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    get_traffic_data();

    if (rank == 0) {
        // Master process
        for (int i = 1; i < size; ++i) {
            MPI_Send(&total_rows, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        }
        
        for (int i = 1; i < size; ++i) {
            MPI_Send(&index_vector[0], total_rows, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(&timestamps[0], total_rows, MPI_CHAR, i, 0, MPI_COMM_WORLD);
            MPI_Send(&traffic_light[0], total_rows, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(&car_counts[0], total_rows, MPI_INT, i, 0, MPI_COMM_WORLD);
        }
    } else {
        // Worker processes
        MPI_Recv(&total_rows, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        index_vector.resize(total_rows);
        timestamps.resize(total_rows);
        traffic_light.resize(total_rows);
        car_counts.resize(total_rows);

        MPI_Recv(&index_vector[0], total_rows, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&timestamps[0], total_rows, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&traffic_light[0], total_rows, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&car_counts[0], total_rows, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    producer_function(rank);
    consumer_function(rank);

    MPI_Finalize();

    return 0;
}
