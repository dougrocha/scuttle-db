use crate::DataType;

#[derive(Debug, Clone)]
pub struct OutputSchema {
    pub fields: Vec<Field>,
}

impl OutputSchema {
    pub fn find_column(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub alias: Option<String>,
    pub data_type: DataType,
    pub is_nullable: bool,
}
