import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * This is just a demo for you,
 * please run it on JDK17 (some statements may be not allowed in lower version).
 * This is just a demo, and you can extend and implement functions
 * based on this demo, or implement it in a different way.

 */
public class OnlineCoursesAnalyzer {

    List<Course> courses = new ArrayList<>();
    public OnlineCoursesAnalyzer(String datasetPath) {
        BufferedReader br = null;
        String line;
        try {
            br = new BufferedReader(new FileReader(datasetPath, StandardCharsets.UTF_8));
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] info = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);
                Course course = new Course(info[0],
                        info[1], new Date(info[2]), info[3], info[4], info[5],
                        Integer.parseInt(info[6]), Integer.parseInt(info[7]),
                        Integer.parseInt(info[8]),
                        Integer.parseInt(info[9]), Integer.parseInt(info[10]), Double.parseDouble(info[11]),
                        Double.parseDouble(info[12]), Double.parseDouble(info[13]),
                        Double.parseDouble(info[14]),
                        Double.parseDouble(info[15]), Double.parseDouble(info[16]),
                        Double.parseDouble(info[17]),
                        Double.parseDouble(info[18]), Double.parseDouble(info[19]),
                        Double.parseDouble(info[20]),
                        Double.parseDouble(info[21]), Double.parseDouble(info[22]));
                courses.add(course);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //1
    public Map<String, Integer> getPtcpCountByInst() {
        Map<String, Integer> mp;
        Stream<Course> courseStream = courses.stream();
        mp = courseStream.collect(Collectors.groupingBy(Course::getInstitution, Collectors.summingInt(Course::getParticipants)));
        mp = mp.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toMap(Map.Entry::getKey,
                Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        return mp;
    }

    //2
    public Map<String, Integer> getPtcpCountByInstAndSubject() {
        Stream<Course> courseStream = courses.stream();
        Map<String, Integer> mp = courseStream.collect(Collectors.groupingBy(v -> v.getInstitution() + "-" + v.getSubject(), Collectors.summingInt(Course::getParticipants)));
        mp = mp.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        return mp;
    }

    //3
    public Map<String, List<List<String>>> getCourseListOfInstructor() {
        return courses.stream().flatMap(course -> Arrays.stream(course.getCleanInstructors().split(",")))
                .map(String::trim)
                .distinct()
                .collect(Collectors.toMap(
                        Function.identity(),
                        instructor -> {
                            List<String> soloCourses = courses.stream()
                                    .filter(course -> course.getInstructors().equals(instructor))
                                    .map(Course::getTitle)
                                    .sorted().distinct()
                                    .collect(Collectors.toList());
                            List<String> coCourses = courses.stream()
                                    .filter(course -> Arrays.stream(course.getInstructors().split(",")).map(String::trim).toList().contains(instructor) && course.getInstructors().split(",").length > 1)
                                    .map(Course::getTitle)
                                    .sorted().distinct()
                                    .collect(Collectors.toList());
                            List<List<String>> coursesList = new ArrayList<>();
                            coursesList.add(soloCourses);
                            coursesList.add(coCourses);
                            return coursesList;
                        }
                ));
    }

    //4
    public List<String> getCourses(int topK, String by) {
        if (by == "hours") {
            return courses.stream().sorted(Comparator.comparingDouble(Course::getTotalHours).reversed().thenComparing(Course::getTitle)).map(Course::getTitle).distinct().limit(topK).collect(Collectors.toList());
//                    .distinct().limit(topK).map(Course::getTitle).collect(Collectors.toList());
        }
        else if (by == "participants") {
            return courses.stream()
                    .sorted(Comparator.comparingInt(Course::getParticipants).reversed().thenComparing(Course::getTitle))
                    .map(Course::getTitle).distinct().limit(topK).collect(Collectors.toList());
        }
        return null;
    }

    //5
    public List<String> searchCourses(String courseSubject, double percentAudited, double totalCourseHours) {
        return courses.stream().filter(course -> course.getSubject().toLowerCase(Locale.ROOT).contains(courseSubject.toLowerCase(Locale.ROOT)) && course.getPercentAudited() >= percentAudited && course.getTotalHours() <= totalCourseHours)
                .map(Course::getTitle).distinct().sorted().collect(Collectors.toList());
    }

    //6
    public List<String> recommendCourses(int age, int gender, int isBachelorOrHigher) {
        return courses.stream()
                .collect(Collectors.groupingBy(Course::getNumber))
                .entrySet().stream()
                .map(entry -> {
                    List<Course> coursesForNumber = entry.getValue();
                    String title = coursesForNumber.stream()
                            .max(Comparator.comparing(Course::getLaunchDate))
                            .get().title;
                    double averageAge = coursesForNumber.stream()
                            .mapToDouble(Course::getMedianAge).average().orElse(999);
                    double averageGender = coursesForNumber.stream()
                            .mapToDouble(Course::getPercentMale).average().orElse(999);
                    double averageDegree = coursesForNumber.stream()
                            .mapToDouble(Course::getPercentBachelorsOrHigher).average().orElse(999);
                    double similarity = similarityValue(age, gender, isBachelorOrHigher,
                            averageAge, averageGender, averageDegree);
                    return Map.entry(title, similarity);
                })
                .sorted((e1, e2) -> {
                    int valueComparison = e1.getValue().compareTo(e2.getValue());
                    return (valueComparison != 0) ? valueComparison : e1.getKey().compareTo(e2.getKey());
                })
                .map(Map.Entry::getKey).distinct()
                .limit(10).collect(Collectors.toList());
    }

    public static void main(String[] args) {
        OnlineCoursesAnalyzer analyzer = new OnlineCoursesAnalyzer("local.csv");
//        System.out.println(analyzer.courses.size());
        List lis = analyzer.getCourses(10, "hours");
        System.out.println(lis);
    }

    public static double similarityValue(int age, int gender, int isBachelorOrHigher, double averageMedianAge, double averageGender, double doubleDegree) {
        return Math.pow(age - averageMedianAge, 2) + Math.pow(gender * 100 - averageGender, 2) + Math.pow(isBachelorOrHigher * 100 - doubleDegree, 2);
    }



}


class Course {
    String institution;
    String number;
    Date launchDate;
    String title;
    String instructors;
    String subject;
    int year;
    int honorCode;
    int participants;
    int audited;
    int certified;
    double percentAudited;
    double percentCertified;
    double percentCertified50;
    double percentVideo;
    double percentForum;
    double gradeHigherZero;
    double totalHours;
    double medianHoursCertification;
    double medianAge;
    double percentMale;
    double percentFemale;
    double percentDegree;

    public Course(String institution, String number, Date launchDate,
                  String title, String instructors, String subject,
                  int year, int honorCode, int participants,
                  int audited, int certified, double percentAudited,
                  double percentCertified, double percentCertified50,
                  double percentVideo, double percentForum, double gradeHigherZero,
                  double totalHours, double medianHoursCertification,
                  double medianAge, double percentMale, double percentFemale,
                  double percentDegree) {
        this.institution = institution;
        this.number = number;
        this.launchDate = launchDate;
        if (title.startsWith("\"")) title = title.substring(1);
        if (title.endsWith("\"")) title = title.substring(0, title.length() - 1);
        this.title = title;
        if (instructors.startsWith("\"")) instructors = instructors.substring(1);
        if (instructors.endsWith("\"")) instructors = instructors.substring(0, instructors.length() - 1);
        this.instructors = instructors;
        if (subject.startsWith("\"")) subject = subject.substring(1);
        if (subject.endsWith("\"")) subject = subject.substring(0, subject.length() - 1);
        this.subject = subject;
        this.year = year;
        this.honorCode = honorCode;
        this.participants = participants;
        this.audited = audited;
        this.certified = certified;
        this.percentAudited = percentAudited;
        this.percentCertified = percentCertified;
        this.percentCertified50 = percentCertified50;
        this.percentVideo = percentVideo;
        this.percentForum = percentForum;
        this.gradeHigherZero = gradeHigherZero;
        this.totalHours = totalHours;
        this.medianHoursCertification = medianHoursCertification;
        this.medianAge = medianAge;
        this.percentMale = percentMale;
        this.percentFemale = percentFemale;
        this.percentDegree = percentDegree;
    }
    public String getInstitution() {
        return institution;
    }
    public int getParticipants() {
        return participants;
    }
    public String getSubject() {
        return subject;
    }
    public String getInstructors() {
        return instructors;
    }
    public String getCleanInstructors() {
        return instructors.replaceAll("\\s*\\([^()]*\\)\\s*", "");
    }
    public String getTitle() {
        return title;
    }
    public double getTotalHours() {
        return totalHours;
    }

    public double getPercentAudited() {
        return percentAudited;
    }
    public double getMedianAge() {
        return medianAge;
    }
    public double getPercentMale() {
        return percentMale;
    }
    public double getPercentBachelorsOrHigher() {
        return percentDegree;
    }
    public String getNumber() {
        return number;
    }
    public Date getLaunchDate() {
        return launchDate;
    }
}
