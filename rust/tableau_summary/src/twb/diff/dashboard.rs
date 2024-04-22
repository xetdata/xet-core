use serde::{Deserialize, Serialize};
use crate::twb::diff::util::{ChangeState, DiffItem, DiffProducer};
use crate::twb::summary::dashboard::{Dashboard, Zone};

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct DashboardDiff {
    pub status: ChangeState,
    pub num_changes: usize,
    pub name: DiffItem<String>,
    pub title: DiffItem<String>,
    pub thumbnail: DiffItem<Option<String>>,
    pub sheets: Vec<DiffItem<String>>,
    pub zones: ZoneDiff,
}

impl DashboardDiff {
    fn update_num_changes(&mut self) {
        if self.name.has_diff() {
            self.num_changes += 1;
        }
        if self.title.has_diff() {
            self.num_changes += 1;
        }
        if self.thumbnail.has_option_diff() {
            self.num_changes += 1;
        }
        self.num_changes += DiffItem::num_list_diffs(&self.sheets);
        self.num_changes += self.zones.num_changes;
    }
}

impl DiffProducer<Dashboard> for DashboardDiff {
    fn new_addition(item: &Dashboard) -> Self {
        let mut diff = DashboardDiff {
            status: ChangeState::Add,
            num_changes: 0,
            name: DiffItem::new_addition(&item.name),
            title: DiffItem::new_addition(&item.title),
            thumbnail: DiffItem::new_addition(&item.thumbnail),
            sheets: DiffItem::new_addition_list(&item.sheets),
            zones: ZoneDiff::new_addition(&item.zones),
        };
        diff.update_num_changes();
        diff
    }

    fn new_deletion(item: &Dashboard) -> Self {
        let mut diff = DashboardDiff {
            status: ChangeState::Delete,
            num_changes: 0,
            name: DiffItem::new_deletion(&item.name),
            title: DiffItem::new_deletion(&item.title),
            thumbnail: DiffItem::new_deletion(&item.thumbnail),
            sheets: DiffItem::new_deletion_list(&item.sheets),
            zones: ZoneDiff::new_deletion(&item.zones),
        };
        diff.update_num_changes();
        diff
    }

    fn new_diff(before: &Dashboard, after: &Dashboard) -> Self {
        let mut diff = DashboardDiff {
            status: ChangeState::Change,
            num_changes: 0,
            name: DiffItem::new_diff(&before.name,&after.name),
            title: DiffItem::new_diff(&before.title,&after.title),
            thumbnail: DiffItem::new_diff(&before.thumbnail,&after.thumbnail),
            sheets: DiffItem::new_diff_list(&before.sheets,&after.sheets),
            zones: ZoneDiff::new_diff(&before.zones,&after.zones),
        };
        diff.update_num_changes();
        if diff.num_changes == 0 {
            diff.status = ChangeState::None
        }
        diff
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ZoneDiff {
    pub status: ChangeState,
    pub num_changes: usize,
    pub name: DiffItem<String>,
    pub zone_type: DiffItem<String>,
    pub is_sheet: DiffItem<bool>,
    pub sub_zones: Vec<ZoneDiff>,
}

impl ZoneDiff {
    fn update_num_changes(&mut self) {
        if self.name.has_diff() {
            self.num_changes += 1;
        }
        if self.zone_type.has_diff() {
            self.num_changes += 1;
        }
        if self.is_sheet.has_diff() {
            self.num_changes += 1;
        }
        self.num_changes += self.sub_zones.iter()
            .map(|z|z.num_changes)
            .sum::<usize>();
    }
}

impl DiffProducer<Zone> for ZoneDiff {
    fn new_addition(item: &Zone) -> Self {
        let mut diff = ZoneDiff {
            status: ChangeState::Add,
            num_changes: 0,
            name: DiffItem::new_addition(&item.name),
            zone_type: DiffItem::new_addition(&item.zone_type),
            is_sheet: DiffItem::new_addition(&item.is_sheet),
            sub_zones: ZoneDiff::new_addition_list(&item.sub_zones),
        };
        diff.update_num_changes();
        diff
    }

    fn new_deletion(item: &Zone) -> Self {
        let mut diff = ZoneDiff {
            status: ChangeState::Delete,
            num_changes: 0,
            name: DiffItem::new_deletion(&item.name),
            zone_type: DiffItem::new_deletion(&item.zone_type),
            is_sheet: DiffItem::new_deletion(&item.is_sheet),
            sub_zones: ZoneDiff::new_deletion_list(&item.sub_zones),
        };
        diff.update_num_changes();
        diff
    }

    fn new_diff(before: &Zone, after: &Zone) -> Self {
        let mut diff = ZoneDiff {
            status: ChangeState::Change,
            num_changes: 0,
            name: DiffItem::new_diff(&before.name,&after.name),
            zone_type: DiffItem::new_diff(&before.zone_type,&after.zone_type),
            is_sheet: DiffItem::new_diff(&before.is_sheet,&after.is_sheet),
            sub_zones: ZoneDiff::new_diff_list(&before.sub_zones,&after.sub_zones),
        };
        diff.update_num_changes();
        if diff.num_changes == 0 {
            diff.status = ChangeState::None
        }
        diff
    }
}

