package com.cyurtoz.web;

import com.cyurtoz.model.PlaybackInfo;
import com.cyurtoz.service.PlaybackService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.List;

@RestController
public class PlaybackController {

    private final PlaybackService playbackService;

    public PlaybackController(PlaybackService playbackService) {
        this.playbackService = playbackService;
    }

    @PostMapping("/playbacks")
    public String startNewPlayback(@RequestParam String sourceTopic,
                                   @RequestParam String destinationTopic,
                                   @RequestParam(defaultValue = "1") double playbackSpeed,
                                   @RequestParam(name = "dateBegin", required = false, defaultValue = "2020-09-22T12:00:00.000+03:00")
                                       @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) Date startDate,
                                   @RequestParam(name = "dateEnd", required = false, defaultValue = "2020-09-22T15:00:00.000+03:00")
                                       @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Date endDate) {
        return playbackService.createNewPlayback(sourceTopic, destinationTopic, playbackSpeed, startDate, endDate);
    }

    @GetMapping("/playbacks")
    public List<PlaybackInfo> listCurrentPlaybacks() {
        return playbackService.listPlaybacks();
    }

    @GetMapping("/playbacks/{playbackId}")
    public void stopPlayback(@PathVariable("playbackId") String playbackId) {
        playbackService.stop(playbackId);
    }
}
