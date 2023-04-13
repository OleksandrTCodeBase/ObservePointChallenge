package ot.challenge.observepointchallenge;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.*;
import java.util.stream.Collectors;

/*
  Data Structures:
  1. I chose HashMap because I had to keep track of an ipAddress and requestCount as a pair.
  Since we have String as a key, it is very efficient to use a hash-based collection.
  2. I considered TreeSet and PriorityQueue, and I chose PriorityQueue because:
    - The time complexity in both will be the same.
    - The space complexity is better in PriorityQueue because TreeSet uses a balanced binary search tree,
    which consumes more memory compared to PriorityQueue.

  How would I test it:
  1. I would create a couple of unit tests for requestHandler, top100, clear
  2. Create a stress test with a large dataset
 */
public class WebServiceRequestListener {
  private final Integer TOP_IP_ADDRESSES_LIMIT = 100;

  private final HashMap<String, IPAddressCount> ipAddressesCountMap;
  private final PriorityQueue<IPAddressCount> topIPAddressesQueue;

  WebServiceRequestListener() {
    ipAddressesCountMap = new HashMap<>();
    topIPAddressesQueue = new PriorityQueue<>(TOP_IP_ADDRESSES_LIMIT);
  }

  /*
    Time Complexity:
      merge - O(1) since we use String as keys
      remove - O(N) (up to 100 elements)
      add, poll - O(log N) (up to 100 elements)
      Since top100IPAddresses is limited to 100 elements, the average-case complexity is O(1)
   */

  /**
   * Handles the request for a given IP address.
   * This method updates the count of requests for the given IP address and maintains the top 100 IP addresses.
   * It is called every time a request is handled by the web service.
   *
   * @param ipAddress A string containing an IP address like "145.87.2.109"
   */
  public void requestHandler(String ipAddress) {
    IPAddressCount ipAddressCount = ipAddressesCountMap.merge(
        ipAddress,
        new IPAddressCount(ipAddress, 1L),
        (updatedIPAddressCount, oldIPAddressCount) ->  {
          updatedIPAddressCount.setCount(updatedIPAddressCount.getCount() + 1);
          return updatedIPAddressCount;
        }
    );

    topIPAddressesQueue.remove(ipAddressCount);
    topIPAddressesQueue.add(ipAddressCount);
    if (topIPAddressesQueue.size() > TOP_IP_ADDRESSES_LIMIT) {
      topIPAddressesQueue.poll();
    }
  }

  /*
    Time Complexity:
      sorted() - O(N log N)
      map() - O(N)
      collect() - O(N)
      Overall time complexity is O(N log N), but since the topIPAddressesQueue size is fixed,
      the time complexity would be O(100 log 100), which is a constant time complexity
   */

  /**
   * Returns the top 100 IP addresses with the highest request count.
   * The IP addresses are sorted in descending order based on the number of requests.
   * Since, in PriorityQueue the order is only guaranteed for the first element (the head), we have to use sorted()
   *
   * @return A collection of the top 100 IP addresses sorted by request count, with the highest traffic IP address first.
   */
  public Collection<String> top100() {
    return topIPAddressesQueue
        .stream()
        .sorted(Comparator.reverseOrder())
        .map(IPAddressCount::getIpAddress)
        .collect(Collectors.toList());
  }

  /*
    Time complexity O(N)
   */

  /**
   * Clears the IP addresses and their request counts.
   * This method is called at the start of each day to reset the tracking of IP addresses and their request counts.
   */
  public void clear() {
    ipAddressesCountMap.clear();
    topIPAddressesQueue.clear();
  }

  @Data
  @AllArgsConstructor
  private static class IPAddressCount implements Comparable<IPAddressCount> {
    private String ipAddress;
    private Long count;

    @Override
    public int compareTo(IPAddressCount other) {
      return Long.compare(other.count, this.count);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ipAddress);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj == null || this.getClass() != obj.getClass()) {
        return false;
      }

      IPAddressCount other = (IPAddressCount) obj;
      return Objects.equals(this.getIpAddress(), other.getIpAddress());
    }
  }
}
