import { useState } from "react";
import { format } from "date-fns";
import { CalendarIcon, Download, X, Check } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { stationsByBorough, boroughs, allStations, type Borough, type StationInfo } from "@/data/stations";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { ThemeToggle } from "@/components/ThemeToggle";

// Create a unique key for each station (name + trains combo)
const getStationKey = (station: StationInfo) => `${station.name}|${station.trains}`;

const Index = () => {
  const [date, setDate] = useState<Date>();
  const [hour, setHour] = useState<string>("12");
  const [minute, setMinute] = useState<string>("00");
  const [selectedStations, setSelectedStations] = useState<Set<string>>(new Set());
  const [selectedBoroughs, setSelectedBoroughs] = useState<Borough[]>([]);
  const [stationOpen, setStationOpen] = useState(false);
  const [isGenerating, setIsGenerating] = useState(false);

  const now = new Date();
  const minDate = new Date("2021-01-01T00:00:00");
  const isToday = date && format(date, "yyyy-MM-dd") === format(now, "yyyy-MM-dd");

  // Get available hours based on selected date
  const getAvailableHours = () => {
    const currentHour = now.getHours();
    if (isToday) {
      return Array.from({ length: currentHour + 1 }, (_, i) =>
        i.toString().padStart(2, "0")
      );
    }
    return Array.from({ length: 24 }, (_, i) =>
      i.toString().padStart(2, "0")
    );
  };

  // Get available minutes based on selected date and hour
  const getAvailableMinutes = () => {
    const allMinutes = ["00", "15", "30", "45"];
    if (isToday && hour === now.getHours().toString().padStart(2, "0")) {
      const currentMinute = now.getMinutes();
      return allMinutes.filter((m) => parseInt(m) <= currentMinute);
    }
    return allMinutes;
  };

  const availableHours = getAvailableHours();
  const availableMinutes = getAvailableMinutes();

  // Reset hour/minute if they become invalid
  const handleDateSelect = (d: Date | undefined) => {
    setDate(d);
    if (d && format(d, "yyyy-MM-dd") === format(now, "yyyy-MM-dd")) {
      const currentHour = now.getHours();
      if (parseInt(hour) > currentHour) {
        setHour(currentHour.toString().padStart(2, "0"));
        setMinute("00");
      }
    }
  };

  const handleHourChange = (h: string) => {
    setHour(h);
    // Reset minute if it's now invalid
    if (isToday && h === now.getHours().toString().padStart(2, "0")) {
      const currentMinute = now.getMinutes();
      if (parseInt(minute) > currentMinute) {
        const validMinutes = ["00", "15", "30", "45"].filter(
          (m) => parseInt(m) <= currentMinute
        );
        setMinute(validMinutes[validMinutes.length - 1] || "00");
      }
    }
  };

  const toggleStation = (station: StationInfo) => {
    const key = getStationKey(station);
    setSelectedStations((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(key)) {
        newSet.delete(key);
      } else {
        newSet.add(key);
      }
      return newSet;
    });
  };

  const toggleBorough = (borough: Borough) => {
    const boroughStations = stationsByBorough[borough];
    const boroughKeys = boroughStations.map(getStationKey);
    const allSelected = boroughKeys.every((key) => selectedStations.has(key));

    if (allSelected) {
      // Remove all stations from this borough
      setSelectedStations((prev) => {
        const newSet = new Set(prev);
        boroughKeys.forEach((key) => newSet.delete(key));
        return newSet;
      });
      setSelectedBoroughs((prev) => prev.filter((b) => b !== borough));
    } else {
      // Add all stations from this borough
      setSelectedStations((prev) => {
        const newSet = new Set(prev);
        boroughKeys.forEach((key) => newSet.add(key));
        return newSet;
      });
      setSelectedBoroughs((prev) => [...prev, borough]);
    }
  };

  const selectAll = () => {
    const allKeys = allStations.map(getStationKey);
    setSelectedStations(new Set(allKeys));
    setSelectedBoroughs([...boroughs]);
  };

  const clearSelection = () => {
    setSelectedStations(new Set());
    setSelectedBoroughs([]);
  };

  const isBoroughFullySelected = (borough: Borough) => {
    const boroughStations = stationsByBorough[borough];
    return boroughStations.every((s) => selectedStations.has(getStationKey(s)));
  };

  const isBoroughPartiallySelected = (borough: Borough) => {
    const boroughStations = stationsByBorough[borough];
    const selectedCount = boroughStations.filter((s) =>
      selectedStations.has(getStationKey(s))
    ).length;
    return selectedCount > 0 && selectedCount < boroughStations.length;
  };

  const getSelectionLabel = () => {
    if (selectedStations.size === 0) return "Select stations...";
    if (selectedStations.size === allStations.length) return "All Stations";
    if (selectedStations.size === 1) {
      const key = Array.from(selectedStations)[0];
      const station = allStations.find(s => getStationKey(s) === key);
      return station ? `${station.name} (${station.trains})` : key;
    }
    return `${selectedStations.size} stations selected`;
  };
  
  const getSelectedStationsList = (): StationInfo[] => {
    return allStations.filter(s => selectedStations.has(getStationKey(s)));
  };

  const handleGenerate = async () => {
    if (!date) {
      toast.error("Please select a date");
      return;
    }

    if (selectedStations.size === 0) {
      toast.error("Please select at least one station");
      return;
    }

    setIsGenerating(true);
    try {
      const apiBase =
        import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";
      const allStations = Object.values(stationsByBorough).flat();
      const selectedList = getSelectedStationsList();
      const dateParam = format(date, "yyyy-MM-dd");
      const timeParam = `${hour}:${minute}`;

      const params = new URLSearchParams({
        format: "xlsx",
        date: dateParam,
        time: timeParam,
      });

      const singleBorough =
        selectedBoroughs.length === 1 ? selectedBoroughs[0] : null;
      const boroughStations = singleBorough
        ? stationsByBorough[singleBorough]
        : [];

      const apiBoroughMap: Record<string, string> = {
        "The Bronx": "Bronx",
        "Staten Island": "Staten Island",
        "Manhattan": "Manhattan",
        "Brooklyn": "Brooklyn",
        "Queens": "Queens",
      };

      if (
        singleBorough &&
        selectedStations.size === boroughStations.length
      ) {
        params.set("borough", apiBoroughMap[singleBorough]);
      } else if (selectedStations.size !== allStations.length) {
        params.set(
          "stations",
          selectedList.map((s) => s.name).join(",")
        );
      }

      const response = await fetch(`${apiBase}/api/report?${params.toString()}`);
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || "Failed to generate report");
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `mta_precp_${dateParam}.xlsx`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);

      const label =
        selectedStations.size === allStations.length
          ? "all stations"
          : `${selectedStations.size} station${selectedStations.size > 1 ? "s" : ""}`;
      toast.success(`Excel generated for ${label}`);
    } catch (error) {
      toast.error(
        error instanceof Error ? error.message : "Failed to generate report"
      );
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <div className="min-h-screen bg-background flex items-center justify-center px-6 relative">
      <ThemeToggle />
      <div className="w-full max-w-md">
        {/* Header */}
        <header className="mb-10 text-center">
          <h1 className="text-4xl md:text-5xl font-semibold tracking-tight text-foreground whitespace-nowrap">
            MTA Rainfall API
          </h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Generate station-level Excel reports by date, time, and location.
          </p>
        </header>

        {/* Main Content */}
        <main className="space-y-8">
          {/* Date */}
          <div className="space-y-3">
            <label className="text-sm font-medium uppercase tracking-wide text-muted-foreground">
              Date
            </label>
              <p className="text-xs text-muted-foreground">
                Historical data available from Jan 1, 2021 to today.
              </p>
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  variant="outline"
                  className={cn(
                    "w-full justify-start text-left font-normal h-12 text-base",
                    !date && "text-muted-foreground"
                  )}
                >
                  <CalendarIcon className="mr-3 h-4 w-4" />
                  {date ? format(date, "MMMM d, yyyy") : "Select a date"}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0 bg-popover" align="start">
                <Calendar
                  mode="single"
                  selected={date}
                  onSelect={handleDateSelect}
                  onTodayClick={() => handleDateSelect(new Date())}
                  disabled={(date) => date > now || date < minDate}
                  initialFocus
                  className="pointer-events-auto"
                />
              </PopoverContent>
            </Popover>
          </div>

          {/* Time */}
          <div className="space-y-3">
            <label className="text-sm font-medium uppercase tracking-wide text-muted-foreground">
              Time
            </label>
            <div className="flex items-center gap-2">
              <Select value={hour} onValueChange={handleHourChange}>
                <SelectTrigger className="w-24 h-12 text-base">
                  <SelectValue placeholder="HH" />
                </SelectTrigger>
                <SelectContent className="bg-popover max-h-60">
                  {availableHours.map((h) => (
                    <SelectItem key={h} value={h}>
                      {h}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <span className="text-xl text-muted-foreground">:</span>
              <Select value={minute} onValueChange={setMinute}>
                <SelectTrigger className="w-24 h-12 text-base">
                  <SelectValue placeholder="MM" />
                </SelectTrigger>
                <SelectContent className="bg-popover">
                  {availableMinutes.map((m) => (
                    <SelectItem key={m} value={m}>
                      {m}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Station / Borough */}
          <div className="space-y-3">
            <div>
              <label className="text-sm font-medium uppercase tracking-wide text-muted-foreground">
                Station / Borough
              </label>
              <p className="text-xs text-muted-foreground mt-1">
                Select multiple stations or entire boroughs
              </p>
            </div>
            <Popover open={stationOpen} onOpenChange={setStationOpen}>
              <PopoverTrigger asChild>
                <Button
                  variant="outline"
                  role="combobox"
                  aria-expanded={stationOpen}
                  className="w-full justify-between h-12 text-base font-normal"
                >
                  <span className="truncate">{getSelectionLabel()}</span>
                </Button>
              </PopoverTrigger>
              <PopoverContent
                className="w-[calc(100vw-3rem)] md:w-96 p-0 bg-popover"
                align="start"
              >
                <Command>
                  <CommandInput placeholder="Search stations or boroughs..." />
                  <div className="flex gap-2 p-2 border-b">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={selectAll}
                      className="flex-1"
                    >
                      Select All
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={clearSelection}
                      className="flex-1"
                    >
                      Clear
                    </Button>
                  </div>
                  <CommandList className="max-h-72">
                    <CommandEmpty>No station found.</CommandEmpty>
                    {boroughs.map((borough) => (
                      <CommandGroup key={borough} heading={borough}>
                        <CommandItem
                          value={`borough-${borough}`}
                          onSelect={() => toggleBorough(borough)}
                          className="cursor-pointer py-2 font-medium"
                        >
                          <div
                            className={cn(
                              "mr-2 flex h-4 w-4 items-center justify-center rounded border border-primary",
                              isBoroughFullySelected(borough)
                                ? "bg-primary text-primary-foreground"
                                : isBoroughPartiallySelected(borough)
                                ? "bg-primary/50"
                                : "opacity-50"
                            )}
                          >
                            {isBoroughFullySelected(borough) && (
                              <Check className="h-3 w-3" />
                            )}
                            {isBoroughPartiallySelected(borough) && (
                              <div className="h-2 w-2 bg-primary-foreground rounded-sm" />
                            )}
                          </div>
                          All {borough} Stations ({stationsByBorough[borough].length})
                        </CommandItem>
                        {stationsByBorough[borough].map((station) => (
                          <CommandItem
                            key={getStationKey(station)}
                            value={`${station.name} ${station.trains}`}
                            onSelect={() => toggleStation(station)}
                            className="cursor-pointer py-2 pl-6"
                          >
                            <div
                              className={cn(
                                "mr-2 flex h-4 w-4 items-center justify-center rounded border border-primary",
                                selectedStations.has(getStationKey(station))
                                  ? "bg-primary text-primary-foreground"
                                  : "opacity-50"
                              )}
                            >
                              {selectedStations.has(getStationKey(station)) && (
                                <Check className="h-3 w-3" />
                              )}
                            </div>
                            <span className="flex-1">{station.name}</span>
                            <span className="ml-2 text-xs text-muted-foreground font-medium">
                              {station.trains}
                            </span>
                          </CommandItem>
                        ))}
                      </CommandGroup>
                    ))}
                  </CommandList>
                </Command>
              </PopoverContent>
            </Popover>

            {/* Selected badges */}
            {selectedStations.size > 0 && selectedStations.size <= 5 && (
              <div className="flex flex-wrap gap-2 pt-2">
                {getSelectedStationsList().map((station) => (
                  <Badge
                    key={getStationKey(station)}
                    variant="secondary"
                    className="flex items-center gap-1"
                  >
                    {station.name} ({station.trains})
                    <X
                      className="h-3 w-3 cursor-pointer hover:text-destructive"
                      onClick={() => toggleStation(station)}
                    />
                  </Badge>
                ))}
              </div>
            )}
          </div>

          {/* Generate Button */}
          <div className="pt-4">
            <Button
              onClick={handleGenerate}
              disabled={isGenerating}
              className="w-full h-12 text-base font-medium"
            >
              {isGenerating ? (
                <>
                  <div className="mr-2 h-4 w-4 animate-spin rounded-full border-2 border-primary-foreground border-t-transparent" />
                  Generating...
                </>
              ) : (
                <>
                  <Download className="mr-2 h-4 w-4" />
                  Generate Excel
                </>
              )}
            </Button>
          </div>
        </main>
      </div>
    </div>
  );
};

export default Index;
