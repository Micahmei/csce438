#ifndef SEM_FILE_H
#define SEM_FILE_H

#include <vector>
#include <string>
#include <semaphore.h>
#include <fcntl.h>
#include <fstream>
#include <unordered_map>
#include <ctime>
#include <sstream>

std::string get_sem_name_from_filepath(const std::string &filepath)
{
    std::string sem_name = filepath;

    // If the filepath starts with './', remove it
    if (sem_name.rfind("./", 0) == 0)
    {
        sem_name = sem_name.substr(2); // Remove './'
    }

    // Replace all '/' with '_' to make it valid for sem_name
    for (char &c : sem_name)
    {
        if (c == '/')
        {
            c = '_'; // Replace '/' with '_'
        }
    }

    // Add a leading '/' to make it a valid sem_name
    sem_name = "/csce438" + sem_name;

    return sem_name;
}

std::vector<std::string> read_users_from_file(const std::string &filename)
{
    std::vector<std::string> users;
    std::ifstream file(filename);
    if (!file)
    {
        return users;
    }

    std::string semName = get_sem_name_from_filepath(filename);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0666, 1);
    if (fileSem == SEM_FAILED)
    {
        perror("sem_open failed");
        return users;
    }
    if (file.peek() == std::ifstream::traits_type::eof())
    {
        sem_close(fileSem);
        return users;
    }

    std::string user;
    while (std::getline(file, user))
    {
        if (!user.empty())
        {
            users.push_back(user);
        }
    }

    sem_close(fileSem);
    return users;
}

bool find_user_from_file(const std::string &filepath, const std::string &username)
{
    std::ifstream file(filepath);
    if (!file)
        return false;

    std::string semName = get_sem_name_from_filepath(filepath);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
    if (fileSem == SEM_FAILED)
    {
        perror("sem_open failed");
        return false;
    }

    if (file.peek() == std::ifstream::traits_type::eof())
    {
        sem_close(fileSem);
        return false;
    }

    bool user_exist = false;
    std::string user;
    while (std::getline(file, user))
    {
        if (!user.empty() && user == username)
            user_exist = true;
    }

    sem_close(fileSem);
    return user_exist;
}
// write a new uesr into file with avoiding repeated
bool write_new_user(const std::string &filename, const std::string &new_user)
{
    std::string semName = get_sem_name_from_filepath(filename);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0666, 1);
    if (fileSem == SEM_FAILED)
    {
        perror("sem_open failed");
        return false;
    }

    sem_wait(fileSem);
    std::ifstream infile(filename);
    bool new_user_exist = false;
    if (infile)
    {
        std::vector<std::string> users;
        std::string user;
        while (std::getline(infile, user))
        {
            if (!user.empty())
                users.push_back(user);
        }
        for (auto u : users)
        {
            if (u == new_user)
                new_user_exist = true;
        }
    }

    if (new_user_exist == false)
    {
        std::ofstream file(filename, std::ios::app);
        if (!file)
        {
            sem_post(fileSem);
            sem_close(fileSem);
            return false;
        }
        file << new_user << std::endl;
        file.close();
        sem_post(fileSem);
        sem_close(fileSem);
        return true;
    }
    else
    {
        sem_post(fileSem);
        sem_close(fileSem);
        return false;
    }
}

void write_follow_time(const std::string &filename, const std::string &follower, const std::string &followee, std::time_t timestamp)
{
    std::string sem_name = get_sem_name_from_filepath(filename);
    sem_t *file_sem = sem_open(sem_name.c_str(), O_CREAT, 0666, 1);
    if (file_sem == SEM_FAILED)
    {
        perror("sem_open failed");
        return;
    }
    sem_wait(file_sem);
    std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> follow_times;
    std::ifstream file(filename);
    bool exist = false;
    if (file)
    {
        std::string line;
        while (std::getline(file, line))
        {
            if (line.empty())
                continue;
            std::istringstream iss(line);
            std::string f, fe, ts_str;
            std::time_t ts;
            // follower, followee, timestamp
            iss >> f >> fe >> ts;
            follow_times[f][fe] = ts;
        }
        file.close();
        if (follow_times.find(follower) != follow_times.end() &&
            follow_times[follower].find(followee) != follow_times[follower].end())
        {
            exist = true;
        }
    }
    if (!exist)
    {
        std::ofstream ofs(filename, std::ios::app);
        if (ofs.is_open())
            ofs << follower << " " << followee << " " << timestamp << "\n";
        ofs.close();
    }
    sem_post(file_sem);
    sem_close(file_sem);
}

std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> read_follow_time(const std::string &filename)
{
    std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> follow_times;

    std::string sem_name = get_sem_name_from_filepath(filename);
    sem_t *file_sem = sem_open(sem_name.c_str(), O_CREAT, 0666, 1);
    if (file_sem == SEM_FAILED)
    {
        perror("sem_open failed");
        return follow_times;
    }

    sem_wait(file_sem);

    std::ifstream file(filename);
    if (!file)
    {
        sem_post(file_sem);
        sem_close(file_sem);
        return follow_times;
    }

    std::string line;
    while (std::getline(file, line))
    {
        if (line.empty())
            continue;
        std::istringstream iss(line);
        std::string f, fe, ts_str;
        std::time_t ts;
        // follower, followee, timestamp
        iss >> f >> fe >> ts;
        follow_times[f][fe] = ts;
    }
    file.close();
    sem_post(file_sem);
    sem_close(file_sem);

    return follow_times;
}

struct Post
{
    std::string username;
    std::string post;
    std::time_t timestamp;
};

// return local time
std::time_t iso8601ToTimeT(const std::string &iso8601)
{
    std::tm tm = {};
    std::istringstream ss(iso8601);

    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");

    if (ss.fail())
    {
        std::cerr << "Error: Failed to parse timestamp: " << iso8601 << std::endl;
        return 0;
    }

    return timegm(&tm);
}

std::vector<Post> read_timeline_from_file(const std::string &filepath)
{
    std::vector<Post> history_posts;
    std::ifstream file(filepath);
    if (!file)
        return history_posts;

    std::string semName = get_sem_name_from_filepath(filepath);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0666, 1);
    if (fileSem == SEM_FAILED)
    {
        perror("sem_open failed");
        return history_posts;
    }
    if (file.peek() == std::ifstream::traits_type::eof())
    {
        sem_close(fileSem);
        return history_posts;
    }

    std::string line, post;

    while (std::getline(file, line))
    {
        if (line.empty() && !post.empty())
        {
            // parse post
            std::istringstream post_stream(post);
            std::string line, post_username, post_content;
            std::time_t post_timestamp;

            while (std::getline(post_stream, line))
            {
                if (line[0] == 'T')
                {
                    post_timestamp = iso8601ToTimeT(line.substr(2));
                }
                else if (line[0] == 'U')
                {
                    post_username = line.substr(2);
                }
                else if (line[0] == 'W')
                {
                    post_content = line.substr(2);
                }
            }
            history_posts.push_back({post_username, post_content, post_timestamp});

            post.clear();
        }
        else
        {
            post += line + "\n";
        }
    }

    sem_close(fileSem);
    return history_posts;
}

void write_timeline_to_file(const std::string &filepath, const std::string &post)
{
    std::string semName = get_sem_name_from_filepath(filepath);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0666, 1);
    if (fileSem == SEM_FAILED)
    {
        perror("sem_open failed");
        return;
    }
    sem_wait(fileSem);
    std::ofstream file(filepath, std::ios::app);
    if (!file)
    {
        sem_post(fileSem);
        sem_close(fileSem);
        return;
    }
    file << post;
    file.close();

    sem_post(fileSem);
    sem_close(fileSem);
}
void write_timeline_to_file(const std::string &filepath, const Post &new_post)
{
    auto posts = read_timeline_from_file(filepath);
    if (!posts.empty())
        for (auto &p : posts)
        {
            if (p.username == new_post.username && p.timestamp == new_post.timestamp)
            {
                return;
            }
        }

    google::protobuf::Timestamp timestamp;
    timestamp.set_seconds(new_post.timestamp);
    timestamp.set_nanos(0);

    std::string formatted =
        "T " + google::protobuf::util::TimeUtil::ToString(timestamp) +
        "\nU " + new_post.username +
        "\nW " + new_post.post + "\n\n";
    write_timeline_to_file(filepath, formatted);
}

#endif
