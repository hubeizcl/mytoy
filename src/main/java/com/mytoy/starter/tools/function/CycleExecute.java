package com.mytoy.starter.tools.function;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class CycleExecute {

    public static <T> List<List<T>> group(List<T> list, Integer pageNum) {
        List<List<T>> result = new ArrayList<>();
        if (null != list && list.size() > 0) {
            int size = list.size();
            int page = size / pageNum;
            int num = size % pageNum;
            for (int i = 0; i < page; i++)
                result.add(list.subList(i * pageNum, (i + 1) * pageNum));
            if (0 != num) result.add(list.subList(page * pageNum, page * pageNum + num));
        }
        return result;
    }

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CycleExecuteBorderless<E> {

        private int gradient = 1;

        private Predicate<E> predicate;

        private Function<Integer, E> function;

        private int init = 0;

        public List<E> execute() {
            List<E> result = new ArrayList<>();
            if (null != predicate && null != function) {
                while (true) {
                    E e = function.apply(init);
                    if (predicate.test(e)) break;
                    result.add(e);
                    init += gradient;
                }
            }
            return result;
        }
    }

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CycleExecuteFunction<T, E> {

        private List<T> list;

        private int pageNum = 50;

        private Predicate<T> predicate;

        private Function<List<T>, List<E>> function;

        private Boolean parallel = false;

        public List<E> execute() {
            List<E> result = new ArrayList<>();
            if (null != list && list.size() > 0 && null != function) {
                List<T> collect = list;
                if (null != predicate) collect = list.stream().filter(predicate).collect(Collectors.toList());
                List<List<T>> newList = CycleExecute.group(collect, pageNum);
                if (null != newList && newList.size() > 0) {
                    Stream<List<T>> stream;
                    if (parallel) stream = newList.parallelStream();
                    else stream = newList.stream();
                    Optional<List<E>> reduce = stream.map(function).reduce((a, b) -> {
                        a.addAll(b);
                        return a;
                    });
                    if (reduce.isPresent()) result = reduce.get();
                }
            } else log.error("集合或者函数为空，请检查！");
            return result;
        }
    }

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CycleExecuteConsumer<T> {

        private List<T> list;

        private int pageNum = 50;

        private Predicate<T> predicate;

        private Consumer<List<T>> consumer;

        private boolean parallel = false;

        public void execute() {
            List<T> collect = list;
            if (null != list && list.size() > 0 && null != consumer) {
                if (null != predicate) collect = list.stream().filter(predicate).collect(Collectors.toList());
                List<List<T>> newList = CycleExecute.group(collect, pageNum);
                if (null != newList && newList.size() > 0) {
                    Stream<List<T>> stream;
                    if (parallel) stream = newList.parallelStream();
                    else stream = newList.stream();
                    stream.forEach(consumer);
                }
            } else log.error("集合或者函数为空，请检查！");
        }
    }
}
